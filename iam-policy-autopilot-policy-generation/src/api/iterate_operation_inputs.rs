//! Iterate through SDK service operations and analyze their input shapes
//!
//! This module provides functionality to iterate through all SDK service operations,
//! analyze their input shapes, and extract detailed information about input parameters.

use crate::embedded_data::BotocoreData;
use anyhow::{Context, Result};
use log::{debug, info, warn};
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::io::Cursor;

/// Information about an operation's input shape member
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputMemberInfo {
    /// Service name (e.g., "s3")
    pub service_name: String,
    /// API version of the service
    pub api_version: String,
    /// Operation name (e.g., "GetObject")
    pub operation_name: String,
    /// Input shape name (e.g., "GetObjectRequest")
    pub input_shape_name: String,
    /// Member name within the input shape
    pub member_name: String,
    /// Whether this member is required
    pub is_required: bool,
    /// The shape name that this member references
    pub member_shape_name: String,
    /// The type of the member shape (e.g., "string", "integer", "structure")
    pub member_shape_type: String,
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

/// Iterate through all SDK service operations and analyze their input shapes
///
/// This function:
/// 1. Discovers all available services
/// 2. For each service, gets the newest API version
/// 3. Loads the service definition
/// 4. For each operation in the service:
///    - Extracts the input shape reference
///    - Analyzes the input shape structure
///    - For each member in the input shape:
///      - Determines if it's required
///      - Gets the member's type
/// 5. Writes the results to JSON and CSV files in the specified output directory
///
/// # Arguments
/// * `output_dir` - Directory where the output files will be written
/// * `pretty` - Whether to format the JSON output with indentation
///
/// # Returns
/// The path to the written JSON output file
pub async fn iterate_operation_inputs(
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

    // Generate output filenames
    let output_file = output_dir.join("operation_inputs_iteration.json");
    let csv_file = output_dir.join("operation_inputs_iteration.csv");

    // Check if files already exist
    if output_file.exists() {
        anyhow::bail!(
            "Output file already exists: {}. Please remove the existing file or choose a different output directory.",
            output_file.display()
        );
    }

    if csv_file.exists() {
        anyhow::bail!(
            "CSV file already exists: {}. Please remove the existing file or choose a different output directory.",
            csv_file.display()
        );
    }

    info!("Starting operation inputs iteration");

    // Discover all services and build service versions map
    let service_versions_map = BotocoreData::build_service_versions_map();
    let service_names: Vec<String> = service_versions_map.keys().cloned().collect();

    info!(
        "Found {} services in service versions map",
        service_names.len()
    );

    let mut all_input_members: Vec<InputMemberInfo> = Vec::new();
    let mut total_operations = 0;
    let mut total_input_members = 0;
    let mut failed_services = Vec::new();

    // Iterate through each service
    for service_name in service_names {
        debug!("Processing service: {}", service_name);

        // Get the newest (last) API version for this service
        let api_versions = match service_versions_map.get(&service_name) {
            Some(versions) => versions,
            None => {
                warn!("No API versions found for service: {}", service_name);
                failed_services.push(service_name.clone());
                continue;
            }
        };

        let api_version = match api_versions.last() {
            Some(version) => version,
            None => {
                warn!("Empty API versions list for service: {}", service_name);
                failed_services.push(service_name.clone());
                continue;
            }
        };

        debug!("  Using API version: {}", api_version);

        // Load the service definition
        let service_def = match BotocoreData::get_service_definition(&service_name, api_version) {
            Ok(def) => def,
            Err(e) => {
                warn!(
                    "Failed to load service definition for {}/{}: {}",
                    service_name, api_version, e
                );
                failed_services.push(service_name.clone());
                continue;
            }
        };

        // Iterate through each operation in the service
        for (operation_name, operation) in &service_def.operations {
            debug!("  Processing operation: {}", operation_name);
            total_operations += 1;

            // Check if the operation has an input shape
            let input_shape_ref = match &operation.input {
                Some(input_ref) => input_ref,
                None => {
                    debug!("    Operation {} has no input shape", operation_name);
                    continue;
                }
            };

            let input_shape_name = &input_shape_ref.shape;

            // Get the input shape from the shapes map
            let input_shape = match service_def.shapes.get(input_shape_name) {
                Some(shape) => shape,
                None => {
                    warn!(
                        "    Input shape {} not found in shapes map for operation {}",
                        input_shape_name, operation_name
                    );
                    continue;
                }
            };

            // Assert that the input shape is a structure
            if input_shape.type_name != "structure" {
                anyhow::bail!(
                    "Expected input shape {} for operation {}:{} to be a structure, but found type: {}",
                    input_shape_name,
                    service_name,
                    operation_name,
                    input_shape.type_name
                );
            }

            // Get the list of required members
            let required_members: Vec<String> = input_shape
                .required
                .as_ref()
                .map(|r| r.clone())
                .unwrap_or_default();

            // Iterate through all members of the input shape
            for (member_name, member_shape_ref) in &input_shape.members {
                let member_shape_name = &member_shape_ref.shape;

                // Check if this member is required
                let is_required = required_members.contains(member_name);

                // Get the member shape to determine its type
                let member_shape_type = match service_def.shapes.get(member_shape_name) {
                    Some(shape) => shape.type_name.clone(),
                    None => {
                        warn!(
                            "      Member shape {} not found in shapes map",
                            member_shape_name
                        );
                        "unknown".to_string()
                    }
                };

                debug!(
                    "    Member: {} (shape: {}, type: {}, required: {})",
                    member_name, member_shape_name, member_shape_type, is_required
                );

                all_input_members.push(InputMemberInfo {
                    service_name: service_name.clone(),
                    api_version: api_version.clone(),
                    operation_name: operation_name.clone(),
                    input_shape_name: input_shape_name.clone(),
                    member_name: member_name.clone(),
                    is_required,
                    member_shape_name: member_shape_name.clone(),
                    member_shape_type,
                });

                total_input_members += 1;
            }
        }
    }

    info!(
        "Operation inputs iteration complete: {} operations, {} input members",
        total_operations, total_input_members
    );

    if !failed_services.is_empty() {
        warn!("Failed to load {} services", failed_services.len());
    }

    // Serialize Vec<InputMemberInfo> to JSON
    let json_output = if pretty {
        serde_json::to_string_pretty(&all_input_members)
    } else {
        serde_json::to_string(&all_input_members)
    }
    .context("Failed to serialize result to JSON")?;

    // Write JSON to file
    std::fs::write(&output_file, &json_output).context(format!(
        "Failed to write output file: {}",
        output_file.display()
    ))?;

    info!(
        "Successfully wrote JSON output to: {}",
        output_file.display()
    );

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
    info!("Writing DataFrame to CSV: {}", csv_file.display());

    let mut csv_file_handle = std::fs::File::create(&csv_file)
        .context(format!("Failed to create CSV file: {}", csv_file.display()))?;

    CsvWriter::new(&mut csv_file_handle)
        .finish(&mut df)
        .context("Failed to write DataFrame to CSV")?;

    info!("Successfully wrote CSV to: {}", csv_file.display());

    Ok(output_file)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_iterate_operation_inputs() {
        // Create temporary directory for output
        let temp_dir = TempDir::new().unwrap();
        let output_path = temp_dir.path().to_path_buf();

        let result = iterate_operation_inputs(output_path, false).await;
        assert!(result.is_ok(), "Failed to iterate: {:?}", result);

        let output_file = result.unwrap();

        // Verify output file was created
        assert!(output_file.exists(), "Output file should exist");

        // Read and parse the JSON file
        let content = std::fs::read_to_string(&output_file).unwrap();
        let input_members: Vec<InputMemberInfo> = serde_json::from_str(&content).unwrap();

        // Verify we got some data
        assert!(
            !input_members.is_empty(),
            "Should have found some input members"
        );

        // Verify data structure
        for member_info in input_members.iter().take(10) {
            // Verify required fields are not empty
            assert!(!member_info.service_name.is_empty());
            assert!(!member_info.api_version.is_empty());
            assert!(!member_info.operation_name.is_empty());
            assert!(!member_info.input_shape_name.is_empty());
            assert!(!member_info.member_name.is_empty());
            assert!(!member_info.member_shape_name.is_empty());
            assert!(!member_info.member_shape_type.is_empty());
        }

        // Verify CSV was also created
        let csv_file = output_file
            .parent()
            .unwrap()
            .join("operation_inputs_iteration.csv");
        assert!(csv_file.exists(), "CSV file should exist");
    }

    #[tokio::test]
    async fn test_iterate_operation_inputs_pretty_json() {
        // Create temporary directory for output
        let temp_dir = TempDir::new().unwrap();
        let output_path = temp_dir.path().to_path_buf();

        let result = iterate_operation_inputs(output_path, true).await;
        assert!(
            result.is_ok(),
            "Failed to iterate with pretty JSON: {:?}",
            result
        );

        let output_file = result.unwrap();

        // Read the JSON file
        let content = std::fs::read_to_string(&output_file).unwrap();

        // Pretty JSON should have newlines
        assert!(content.contains('\n'), "Pretty JSON should have newlines");
    }

    #[tokio::test]
    async fn test_iterate_operation_inputs_output_dir_not_exists() {
        let non_existent_path = std::path::PathBuf::from("/non/existent/directory");

        let result = iterate_operation_inputs(non_existent_path, false).await;
        assert!(result.is_err(), "Should fail for non-existent directory");

        if let Err(e) = result {
            let error_msg = format!("{}", e);
            assert!(
                error_msg.contains("Output directory does not exist"),
                "Error should mention directory not existing: {}",
                error_msg
            );
        }
    }

    #[tokio::test]
    async fn test_iterate_operation_inputs_file_already_exists() {
        // Create temporary directory for output
        let temp_dir = TempDir::new().unwrap();
        let output_path = temp_dir.path().to_path_buf();

        // Create the output file first
        let output_file = output_path.join("operation_inputs_iteration.json");
        std::fs::write(&output_file, "dummy content").unwrap();

        let result = iterate_operation_inputs(output_path, false).await;
        assert!(result.is_err(), "Should fail when output file exists");

        if let Err(e) = result {
            let error_msg = format!("{}", e);
            assert!(
                error_msg.contains("already exists"),
                "Error should mention file already exists: {}",
                error_msg
            );
        }
    }

    #[tokio::test]
    async fn test_input_member_info_structure() {
        // Create temporary directory for output
        let temp_dir = TempDir::new().unwrap();
        let output_path = temp_dir.path().to_path_buf();

        let output_file = iterate_operation_inputs(output_path, false)
            .await
            .expect("Failed to iterate");

        // Read and parse the JSON file
        let content = std::fs::read_to_string(&output_file).unwrap();
        let input_members: Vec<InputMemberInfo> = serde_json::from_str(&content).unwrap();

        // Verify we found input members
        assert!(
            !input_members.is_empty(),
            "Should find at least one input member"
        );

        // Verify structure of first member
        let member = &input_members[0];
        assert!(!member.service_name.is_empty());
        assert!(!member.operation_name.is_empty());
        assert!(!member.member_name.is_empty());

        // Verify that we have both required and non-required members
        let has_required = input_members.iter().any(|m| m.is_required);
        let has_optional = input_members.iter().any(|m| !m.is_required);

        if input_members.len() > 1 {
            // Only check if we have more than one member
            assert!(
                has_required || has_optional,
                "Should have at least some required or optional members"
            );
        }
    }
}
