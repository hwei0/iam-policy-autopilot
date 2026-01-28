//! Iterate through service reference files and their operations
//!
//! This module provides functionality to iterate through all service reference files,
//! their operations, authorized actions, and retrieve full action information.

use crate::{
    enrichment::service_reference::{
        Action, AuthorizedAction, RemoteServiceReferenceLoader, SdkMethod,
    },
    policy_generation::utils::get_placeholder_regex,
};
use anyhow::{Context, Result};
use itertools::Itertools;
use log::{debug, info, warn};
use polars::{prelude::*, time::prelude::string::infer};
use regex::Captures;
use serde::{Deserialize, Serialize};
use std::io::Cursor;

/// Resource information with ARN formats
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceInfo {
    /// Resource name (e.g., "bucket", "object")
    pub name: String,

    /// ARN format patterns for this resource
    pub arn: Vec<ArnTemplateInfo>,
}

/// Resource information with ARN formats
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArnTemplateInfo {
    arn_template: String,
    arn_variables: Vec<String>,
}

impl ArnTemplateInfo {
    pub fn new(arn_template: String) -> Self {
        let mut variable_list = Vec::<String>::new();

        let regex = get_placeholder_regex();

        let _ = regex
            .replace_all(&arn_template, |caps: &Captures| {
                match caps.get(1).map(|m| m.as_str()) {
                    Some(placeholder) => {
                        match placeholder.to_lowercase().as_str() {
                            "partition" => "*",
                            "region" => "*",
                            "account" => "*",
                            _ => {
                                variable_list.push(placeholder.to_string());
                                "*" // All other variables become wildcards
                            }
                        }
                    }
                    None => {
                        "*" // Fallback (should not happen due to validation)
                    }
                }
            })
            .to_string();

        ArnTemplateInfo {
            arn_template: arn_template,
            arn_variables: variable_list,
        }
    }
}

/// Enriched action with resource details
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnrichedAction {
    /// The original action
    #[serde(flatten)]
    pub action: Action,
    /// Enriched resource information
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_details: Option<Vec<ResourceInfo>>,
}

/// Information about an operation and its SDK methods
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationInfo {
    /// The service anme, e.g. "s3"
    pub service_name: String,
    /// The operation name (e.g., "s3:GetObject")
    pub operation_name: String,
    /// SDK methods associated with this operation
    pub sdk_methods: Vec<SdkMethod>,
    /// Authorized actions for this operation
    pub authorized_actions: Vec<AuthorizedActionInfo>,
}

/// Information about an authorized action with full action details
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthorizedActionInfo {
    /// The authorized action NOTE; this might be a duplicate of what is in action_details
    pub authorized_action: AuthorizedAction,
    /// Full action details from the service reference, enriched with resource information
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action_details: Option<EnrichedAction>,
}

/// Recursively flatten a DataFrame by expanding all struct columns and exploding list columns
///
/// This function iterates through all columns and:
/// 1. Flattens any Struct columns by extracting their fields
/// 2. Explodes any List columns to create multiple rows
/// It then recursively calls itself until no more nested types remain.
///
/// # Arguments
/// * `df` - The DataFrame to flatten
///
/// # Returns
/// A flattened DataFrame with no nested struct or list columns
///
/// You could have just use #[serde(flatten)] to simplify stuff, you dumbass
fn flatten_dataframe_recursively(mut df: DataFrame) -> Result<DataFrame> {
    loop {
        let mut has_nested = false;
        let schema = df.schema();

        // Check if any columns are structs or lists
        for field in schema.iter_fields() {
            if matches!(field.dtype(), DataType::Struct(_) | DataType::List(_)) {
                has_nested = true;
                break;
            }
        }

        // If no nested columns, we're done
        if !has_nested {
            break;
        }

        // First, explode any list columns
        for field in schema.clone().iter_fields() {
            if matches!(field.dtype(), DataType::List(_)) {
                debug!("Exploding list column: {}", field.name());
                df = df
                    .explode([field.name().clone()])
                    .context(format!("Failed to explode column {}", field.name()))?
                    .clone();
                // After exploding, we need to re-check the schema
                break;
            }
        }

        // Then flatten struct columns
        let schema = df.schema();
        let mut columns_to_add = Vec::new();
        let mut columns_to_remove = Vec::new();

        for (col_idx, field) in schema.iter_fields().enumerate() {
            if let DataType::Struct(fields) = field.dtype() {
                debug!("Flattening struct column: {}", field.name());
                columns_to_remove.push(field.name().to_string());

                let series = &df.get_columns()[col_idx];

                // Extract each field from the struct
                for (field_idx, struct_field) in fields.iter().enumerate() {
                    let field_name = format!("{}-{}", field.name(), struct_field.name());

                    // Extract the field as a new series
                    if let Ok(struct_chunked) = series.struct_() {
                        if let Some(field_series) = struct_chunked.fields_as_series().get(field_idx)
                        {
                            columns_to_add.push((field_name, field_series.clone()));
                        }
                    }
                }
            }
        }

        // Remove struct columns and add flattened columns
        for col_name in &columns_to_remove {
            df = df
                .drop(col_name.as_str())
                .context(format!("Failed to drop column {}", col_name))?;
        }

        for (col_name, series) in columns_to_add {
            df = df
                .with_column(series.with_name(col_name.as_str().into()))
                .context(format!("Failed to add column {}", col_name))?
                .clone();
        }
    }

    Ok(df)
}

/// Iterate through all service reference files and their operations
///
/// This function:
/// 1. Initializes a RemoteServiceReferenceLoader
/// 2. Gets the service reference mapping
/// 3. For each service in the mapping, loads the service reference
/// 4. For each operation in the service reference:
///    - Extracts the SDK methods
///    - For each authorized action, retrieves the full action details
/// 5. Writes the results to a JSON file in the specified output directory
///
/// # Arguments
/// * `output_dir` - Directory where the JSON output file will be written
/// * `pretty` - Whether to format the JSON output with indentation
///
/// # Returns
/// The path to the written output file
pub async fn iterate_service_references(
    output_dir: std::path::PathBuf,
    pretty: bool,
) -> Result<std::path::PathBuf> {
    // Validate output directory exists
    if !output_dir.exists() {
        anyhow::bail!("Output directory does not exist: {}", output_dir.display());
    }

    if !output_dir.is_dir() {
        anyhow::bail!("Output path is not a directory: {}", output_dir.display());
    }

    // Generate output filename
    let output_file = output_dir.join("service_references_iteration.json");

    // Check if file already exists
    if output_file.exists() {
        anyhow::bail!(
            "Output file already exists: {}. Please remove the existing file or choose a different output directory.",
            output_file.display()
        );
    }
    info!("Starting service reference iteration");

    // Initialize the RemoteServiceReferenceLoader
    let loader = RemoteServiceReferenceLoader::new(false)
        .context("Failed to initialize RemoteServiceReferenceLoader")?;

    // Get the service reference mapping
    let mapping = loader
        .get_or_init_mapping()
        .await
        .context("Failed to get service reference mapping")?;

    let service_names: Vec<String> = mapping.service_reference_mapping.keys().cloned().collect();

    info!(
        "Found {} services in service reference mapping",
        service_names.len()
    );

    let mut all_operations: Vec<OperationInfo> = Vec::new();
    let mut total_operations = 0;
    let mut total_authorized_actions = 0;
    let mut failed_services = Vec::new();

    // Iterate through each service
    for service_name in service_names {
        debug!("Processing service: {}", service_name);

        // Load the service reference
        let service_ref = match loader.load(&service_name).await {
            Ok(Some(service_ref)) => service_ref,
            Ok(None) => {
                warn!("Service reference not found for: {}", service_name);
                failed_services.push(service_name.clone());
                continue;
            }
            Err(e) => {
                warn!(
                    "Failed to load service reference for {}: {}",
                    service_name, e
                );
                failed_services.push(service_name.clone());
                continue;
            }
        };

        // Check if this service has operation_to_authorized_actions
        let operations = match &service_ref.operation_to_authorized_actions {
            Some(operations) => operations,
            None => {
                debug!(
                    "Service {} has no operation_to_authorized_actions",
                    service_name
                );
                continue;
            }
        };

        // Iterate through each operation in the service
        for (operation_name, operation) in operations {
            debug!("  Processing operation: {}", operation_name);

            // Store the SDK methods
            let sdk_methods = operation.sdk.clone();

            // Process each authorized action
            let mut authorized_action_infos = Vec::new();

            for authorized_action in &operation.authorized_actions {
                debug!(
                    "    Processing authorized action: {}:{}",
                    authorized_action.service, authorized_action.name
                );

                // Load the service reference for this authorized action's service
                let action_details = match get_action_details(
                    &loader,
                    &authorized_action.service,
                    &authorized_action.name,
                )
                .await
                {
                    Ok(details) => details,
                    Err(e) => {
                        warn!(
                            "Failed to get action details for {}:{}: {}",
                            authorized_action.service, authorized_action.name, e
                        );
                        None
                    }
                };

                authorized_action_infos.push(AuthorizedActionInfo {
                    authorized_action: authorized_action.clone(),
                    action_details,
                });

                total_authorized_actions += 1;
            }

            all_operations.push(OperationInfo {
                service_name: service_ref.service_name.clone(),
                operation_name: operation_name.clone(),
                sdk_methods,
                authorized_actions: authorized_action_infos,
            });

            total_operations += 1;
        }
    }

    info!(
        "Service reference iteration complete: {} operations, {} authorized actions",
        total_operations, total_authorized_actions
    );

    if !failed_services.is_empty() {
        warn!("Failed to load {} services", failed_services.len());
    }

    // Serialize Vec<OperationInfo> to JSON
    let json_output = if pretty {
        serde_json::to_string_pretty(&all_operations)
    } else {
        serde_json::to_string(&all_operations)
    }
    .context("Failed to serialize result to JSON")?;

    // Write to file
    std::fs::write(&output_file, &json_output).context(format!(
        "Failed to write output file: {}",
        output_file.display()
    ))?;

    info!("Successfully wrote output to: {}", output_file.display());

    // Create DataFrame from JSON using JsonReader
    info!("Creating DataFrame from JSON content");
    let cursor = Cursor::new(json_output.as_bytes());
    let mut df = JsonReader::new(cursor)
        .infer_schema_len(None)
        .finish()
        .context("Failed to create DataFrame from JSON")?;

    info!(
        "Successfully created DataFrame with {} rows and {} columns",
        df.height(),
        df.width()
    );

    // Log initial DataFrame schema
    info!("Initial DataFrame schema:");
    for field in df.schema().iter_fields() {
        info!("  - {} ({})", field.name(), field.dtype());
    }

    // Recursively flatten the DataFrame
    info!("Flattening DataFrame...");
    df = flatten_dataframe_recursively(df).context("Failed to flatten DataFrame")?;

    info!(
        "Flattened DataFrame with {} rows and {} columns",
        df.height(),
        df.width()
    );

    // Log flattened DataFrame schema
    info!("Flattened DataFrame schema:");
    for field in df.schema().iter_fields() {
        info!("  - {} ({})", field.name(), field.dtype());
    }

    // Write DataFrame to CSV file
    let csv_file = output_dir.join("service_references_iteration.csv");
    info!("Writing DataFrame to CSV: {}", csv_file.display());

    let mut csv_file_handle = std::fs::File::create(&csv_file)
        .context(format!("Failed to create CSV file: {}", csv_file.display()))?;

    CsvWriter::new(&mut csv_file_handle)
        .finish(&mut df)
        .context("Failed to write DataFrame to CSV")?;

    info!("Successfully wrote CSV to: {}", csv_file.display());

    Ok(output_file)
}

/// Helper function to get action details for a specific service and action name,
/// enriched with resource information from the service reference
///
/// # Arguments
/// * `loader` - The RemoteServiceReferenceLoader
/// * `service_name` - The service name (e.g., "s3")
/// * `action_name` - The action name (e.g., "s3:GetObject")
///
/// # Returns
/// The EnrichedAction details if found, or None if not available
async fn get_action_details(
    loader: &RemoteServiceReferenceLoader,
    service_name: &str,
    action_name: &str,
) -> Result<Option<EnrichedAction>> {
    // Load the service reference
    let service_ref = loader.load(service_name).await.context(format!(
        "Failed to load service reference for {}",
        service_name
    ))?;

    let service_ref = match service_ref {
        Some(sr) => sr,
        None => return Ok(None),
    };

    // Extract just the action name part (after the colon)
    // action_name is in format "service:action" (e.g., "s3:GetObject")
    let action_key = if let Some(idx) = action_name.rfind(':') {
        &action_name[idx + 1..]
    } else {
        action_name
    };

    // Look up the action in the actions HashMap
    let action = match service_ref.actions.get(action_key).cloned() {
        Some(action) => action,
        None => return Ok(None),
    };

    // Enrich the action with resource details
    let resource_details = if !action.resources.is_empty() {
        let mut details = Vec::new();
        for resource_name in &action.resources {
            if let Some(arn_template_list) = service_ref.resources.get(resource_name) {
                details.push(ResourceInfo {
                    name: resource_name.clone(),
                    arn: arn_template_list
                        .iter()
                        .map(|arn_template| ArnTemplateInfo::new(arn_template.clone()))
                        .collect_vec(),
                });
            }
        }
        if details.is_empty() {
            None
        } else {
            Some(details)
        }
    } else {
        None
    };

    Ok(Some(EnrichedAction {
        action,
        resource_details,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::enrichment::mock_remote_service_reference;
    use std::path::PathBuf;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_iterate_service_references() {
        let (_server, _loader) =
            mock_remote_service_reference::setup_mock_server_with_loader().await;

        // Create temporary directory for output
        let temp_dir = TempDir::new().unwrap();
        let output_path = temp_dir.path().to_path_buf();

        let result = iterate_service_references(output_path, false).await;
        assert!(result.is_ok(), "Failed to iterate: {:?}", result);

        let output_file = result.unwrap();

        // Verify output file was created
        assert!(output_file.exists(), "Output file should exist");

        // Read and parse the JSON file
        let content = std::fs::read_to_string(&output_file).unwrap();
        let operations: Vec<OperationInfo> = serde_json::from_str(&content).unwrap();

        // Verify we got some data
        assert!(!operations.is_empty(), "Should have found some operations");

        // Verify data structure
        for operation in &operations {
            // Verify operation has a name
            assert!(!operation.operation_name.is_empty());
            assert!(!operation.service_name.is_empty());

            // Verify we have authorized actions
            assert!(
                !operation.authorized_actions.is_empty(),
                "Operation {} has no authorized actions",
                operation.operation_name
            );
        }
    }

    #[tokio::test]
    async fn test_get_action_details() {
        let (_, loader) = mock_remote_service_reference::setup_mock_server_with_loader().await;

        // Test getting action details for s3:GetObject
        let result = get_action_details(&loader, "s3", "s3:GetObject").await;
        assert!(result.is_ok());

        let enriched_action = result.unwrap();
        assert!(
            enriched_action.is_some(),
            "Should find GetObject action for s3"
        );

        let enriched_action = enriched_action.unwrap();
        assert_eq!(enriched_action.action.name, "GetObject");

        // Verify resource details are enriched
        if !enriched_action.action.resources.is_empty() {
            assert!(
                enriched_action.resource_details.is_some(),
                "Should have resource details when action has resources"
            );
        }
    }

    #[tokio::test]
    async fn test_get_action_details_nonexistent() {
        let (_, loader) = mock_remote_service_reference::setup_mock_server_with_loader().await;

        // Test getting action details for a non-existent action
        let result = get_action_details(&loader, "s3", "s3:NonExistentAction").await;
        assert!(result.is_ok());

        let action = result.unwrap();
        assert!(action.is_none(), "Should not find non-existent action");
    }

    #[tokio::test]
    async fn test_operation_info_structure() {
        let (_server, _loader) =
            mock_remote_service_reference::setup_mock_server_with_loader().await;

        // Create temporary directory for output
        let temp_dir = TempDir::new().unwrap();
        let output_path = temp_dir.path().to_path_buf();

        let output_file = iterate_service_references(output_path, false)
            .await
            .expect("Failed to iterate");

        // Read and parse the JSON file
        let content = std::fs::read_to_string(&output_file).unwrap();
        let operations: Vec<OperationInfo> = serde_json::from_str(&content).unwrap();

        // Verify we found operations
        assert!(!operations.is_empty(), "Should find at least one operation");

        // Verify structure of first operation
        let operation = &operations[0];
        assert!(!operation.operation_name.is_empty());
        assert!(!operation.service_name.is_empty());

        // Verify authorized actions
        for auth_action_info in &operation.authorized_actions {
            assert!(!auth_action_info.authorized_action.name.is_empty());
            assert!(!auth_action_info.authorized_action.service.is_empty());
        }
    }
}
