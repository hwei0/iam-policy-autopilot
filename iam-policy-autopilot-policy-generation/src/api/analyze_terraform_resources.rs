use anyhow::{Context, Result};
use itertools::Itertools;
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

use crate::api::{extract_sdk_calls, model::ExtractSdkCallsConfig};
use crate::extraction::SdkMethodCall;
use crate::ExtractedMethods;

/// Represents the location of a function declaration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct FuncDecl {
    /// The file containing the function declaration
    pub filename: String,
    /// Byte offset in the file
    pub offset: i32,
    /// Line number in the file
    pub line: i32,
    /// Column number in the line
    pub column: i32,
}

/// Metadata structure for Terraform resource extraction
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct MetadataStruct {
    /// AWS service directory name
    pub service_dir_name: String,
    /// Path to the Terraform provider file
    pub file_path: String,
    /// Terraform resource name
    pub terraform_resource_name: String,
    /// SDK resource name
    pub sdk_resource_name: String,
    /// Name of the create function
    pub create_function_name: String,
    /// Resource decorator annotations
    pub resource_decorators: Vec<String>,
    /// Row number of the first API call
    pub first_call_row: i32,
    /// Column number of the first API call
    pub first_call_col: i32,
    /// Row number of the last API call
    pub last_call_row: i32,
    /// Column number of the last API call
    pub last_call_col: i32,
    /// Function declarations before the create function
    pub before_func_decls: Vec<FuncDecl>,
    /// Function declarations between API calls in the create function
    pub intermediate_func_decls: Vec<FuncDecl>,
    /// Function declarations after the create function
    pub after_func_decls: Vec<FuncDecl>,
    /// All function declarations in the file
    pub all_func_decls: Vec<FuncDecl>,
}

/// File paths for a single resource's extracted data
#[derive(Debug, Clone)]
pub struct ResourceFiles {
    /// Path to after_calls.go
    pub after_calls: PathBuf,
    /// Path to before_calls.go
    pub before_calls: PathBuf,
    /// Path to create_function_calls.go
    pub create_function_calls: PathBuf,
    /// Path to create_function_only.go
    pub create_function_only: PathBuf,
    /// Path to intermediate_calls.go
    pub intermediate_calls: PathBuf,
    /// Parsed metadata from metadata.json
    pub metadata: MetadataStruct,
}

/// Extracted SDK calls for a single resource
#[derive(Debug, Clone)]
pub struct ResourceExtractedCalls {
    /// Extracted methods from after_calls.go
    pub after_calls: ExtractedMethods,
    /// Extracted methods from before_calls.go
    pub before_calls: ExtractedMethods,
    /// Extracted methods from create_function_calls.go
    pub create_function_calls: ExtractedMethods,
    /// Extracted methods from create_function_only.go
    pub create_function_only: ExtractedMethods,
    /// Extracted methods from intermediate_calls.go
    pub intermediate_calls: ExtractedMethods,
    /// Parsed metadata from metadata.json
    pub metadata: MetadataStruct,
}

/// A single row of data for the analysis DataFrame
#[derive(Debug, Clone)]
pub struct ResourceAnalysisRow {
    /// AWS service name
    pub service_name: String,
    /// Terraform resource name
    pub terraform_resource_name: String,
    /// AWS SDK resource name
    pub aws_sdk_resource_name: String,
    /// Number of before SDK calls
    pub num_before_sdk_calls: i32,
    /// Comma-separated list of before SDK calls
    pub before_sdk_calls: String,
    /// Number of intermediate SDK calls
    pub num_intermediate_sdk_calls: i32,
    /// Comma-separated list of intermediate SDK calls
    pub intermediate_sdk_calls: String,
    /// Number of after SDK calls
    pub num_after_sdk_calls: i32,
    /// Comma-separated list of after SDK calls
    pub after_sdk_calls: String,
    /// Number of create function stack calls
    pub num_create_function_stack_calls: i32,
    /// Comma-separated list of create function stack calls
    pub create_function_stack_calls: String,
    /// Number of create function only calls
    pub num_create_function_only_calls: i32,
    /// Comma-separated list of create function only calls
    pub create_function_only_calls: String,
    /// Row number of the first API call
    pub first_call_row: i32,
    /// Column number of the first API call
    pub first_call_col: i32,
    /// Row number of the last API call
    pub last_call_row: i32,
    /// Column number of the last API call
    pub last_call_col: i32,
    /// File path
    pub file_path: String,
}

/// Collection of column vectors for building a DataFrame
#[derive(Debug, Default)]
pub struct ResourceAnalysisColumns {
    /// Service names column
    pub service_names: Vec<String>,
    /// Terraform resource names column
    pub terraform_resource_names: Vec<String>,
    /// AWS SDK resource names column
    pub aws_sdk_resource_names: Vec<String>,
    /// Number of before SDK calls column
    pub num_before_sdk_calls: Vec<i32>,
    /// Before SDK calls column (comma-separated strings)
    pub before_sdk_calls: Vec<String>,
    /// Number of intermediate SDK calls column
    pub num_intermediate_sdk_calls: Vec<i32>,
    /// Intermediate SDK calls column (comma-separated strings)
    pub intermediate_sdk_calls: Vec<String>,
    /// Number of after SDK calls column
    pub num_after_sdk_calls: Vec<i32>,
    /// After SDK calls column (comma-separated strings)
    pub after_sdk_calls: Vec<String>,
    /// Number of create function stack calls column
    pub num_create_function_stack_calls: Vec<i32>,
    /// Create function stack calls column (comma-separated strings)
    pub create_function_stack_calls: Vec<String>,
    /// Number of create function only calls column
    pub num_create_function_only_calls: Vec<i32>,
    /// Create function only calls column (comma-separated strings)
    pub create_function_only_calls: Vec<String>,
    /// Row number of the first API call column
    pub first_call_row: Vec<i32>,
    /// Column number of the first API call column
    pub first_call_col: Vec<i32>,
    /// Row number of the last API call column
    pub last_call_row: Vec<i32>,
    /// Column number of the last API call column
    pub last_call_col: Vec<i32>,
    /// File paths column
    pub file_paths: Vec<String>,
}

impl ResourceAnalysisColumns {
    /// Create a new empty columns collection
    pub fn new() -> Self {
        Self::default()
    }

    /// Append a row to the columns
    pub fn append(&mut self, row: ResourceAnalysisRow) {
        self.service_names.push(row.service_name);
        self.terraform_resource_names
            .push(row.terraform_resource_name);
        self.aws_sdk_resource_names.push(row.aws_sdk_resource_name);
        self.num_before_sdk_calls.push(row.num_before_sdk_calls);
        self.before_sdk_calls.push(row.before_sdk_calls);
        self.num_intermediate_sdk_calls
            .push(row.num_intermediate_sdk_calls);
        self.intermediate_sdk_calls.push(row.intermediate_sdk_calls);
        self.num_after_sdk_calls.push(row.num_after_sdk_calls);
        self.after_sdk_calls.push(row.after_sdk_calls);
        self.num_create_function_stack_calls
            .push(row.num_create_function_stack_calls);
        self.create_function_stack_calls
            .push(row.create_function_stack_calls);
        self.num_create_function_only_calls
            .push(row.num_create_function_only_calls);
        self.create_function_only_calls
            .push(row.create_function_only_calls);
        self.first_call_row.push(row.first_call_row);
        self.first_call_col.push(row.first_call_col);
        self.last_call_row.push(row.last_call_row);
        self.last_call_col.push(row.last_call_col);
        self.file_paths.push(row.file_path);
    }

    /// Convert the columns into a Polars DataFrame
    pub fn to_dataframe(self) -> PolarsResult<DataFrame> {
        DataFrame::new(vec![
            Column::new("service_name".into(), self.service_names),
            Column::new(
                "terraform_resource_name".into(),
                self.terraform_resource_names,
            ),
            Column::new("aws_sdk_resource_name".into(), self.aws_sdk_resource_names),
            Column::new("num_before_sdk_calls".into(), self.num_before_sdk_calls),
            Column::new("before_sdk_calls".into(), self.before_sdk_calls),
            Column::new(
                "num_intermediate_sdk_calls".into(),
                self.num_intermediate_sdk_calls,
            ),
            Column::new("intermediate_sdk_calls".into(), self.intermediate_sdk_calls),
            Column::new("num_after_sdk_calls".into(), self.num_after_sdk_calls),
            Column::new("after_sdk_calls".into(), self.after_sdk_calls),
            Column::new(
                "num_create_function_stack_calls".into(),
                self.num_create_function_stack_calls,
            ),
            Column::new(
                "create_function_stack_calls".into(),
                self.create_function_stack_calls,
            ),
            Column::new(
                "num_create_function_only_calls".into(),
                self.num_create_function_only_calls,
            ),
            Column::new(
                "create_function_only_calls".into(),
                self.create_function_only_calls,
            ),
            Column::new("first_call_row".into(), self.first_call_row),
            Column::new("first_call_col".into(), self.first_call_col),
            Column::new("last_call_row".into(), self.last_call_row),
            Column::new("last_call_col".into(), self.last_call_col),
            Column::new("file_path".into(), self.file_paths),
        ])
    }
}

/// Extract SDK calls from Terraform resource directories and write to JSON files
///
/// Iterates through subdirectories in the resource extractor output directory,
/// loads the 5 go files and metadata.json for each resource, extracts SDK calls,
/// and writes the extracted methods to JSON files in the same subdirectories.
///
/// # Arguments
///
/// * `resource_extractor_output` - Path to the directory containing resource subdirectories
///
/// # Returns
///
/// Returns `Ok(())` on success, or an error if directory reading, SDK extraction, or file writing fails
pub async fn extract_terraform_resource_sdk_calls(
    resource_extractor_output: PathBuf,
) -> Result<()> {
    // Read all subdirectories in the resource_extractor_output directory
    let entries = fs::read_dir(&resource_extractor_output)
        .with_context(|| format!("Failed to read directory: {:?}", resource_extractor_output))?;

    for entry in entries {
        let entry = entry.with_context(|| "Failed to read directory entry")?;
        let path = entry.path();

        // Skip if not a directory
        if !path.is_dir() {
            continue;
        }

        // Extract resource name from directory name
        let resource_name = path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| anyhow::anyhow!("Invalid directory name: {:?}", path))?;

        // Build paths to the 5 go files
        let after_calls = path.join("after_calls.go");
        let before_calls = path.join("before_calls.go");
        let create_function_calls = path.join("create_function_calls.go");
        let create_function_only = path.join("create_function_only.go");
        let intermediate_calls = path.join("intermediate_calls.go");
        let metadata_path = path.join("metadata.json");

        // Verify all files exist
        if !after_calls.exists()
            || !before_calls.exists()
            || !create_function_calls.exists()
            || !create_function_only.exists()
            || !intermediate_calls.exists()
            || !metadata_path.exists()
        {
            eprintln!(
                "Warning: Skipping directory {:?} - missing required files",
                path
            );
            continue;
        }

        // Load and deserialize metadata.json using serde_json
        let metadata_file = fs::File::open(&metadata_path)
            .with_context(|| format!("Failed to open metadata file: {:?}", metadata_path))?;

        let metadata: MetadataStruct = serde_json::from_reader(metadata_file)
            .with_context(|| format!("Failed to deserialize metadata JSON: {:?}", metadata_path))?;

        // Extract SDK calls from after_calls.go
        let after_calls_config = ExtractSdkCallsConfig {
            source_files: vec![after_calls.clone()],
            language: Some("go".to_string()),
            service_hints: None,
        };
        let after_calls_methods = extract_sdk_calls::extract_sdk_calls(&after_calls_config)
            .await
            .with_context(|| {
                format!(
                    "Failed to extract SDK calls from after_calls.go: {:?}",
                    after_calls
                )
            })?;

        // Extract SDK calls from before_calls.go
        let before_calls_config = ExtractSdkCallsConfig {
            source_files: vec![before_calls.clone()],
            language: Some("go".to_string()),
            service_hints: None,
        };
        let before_calls_methods = extract_sdk_calls::extract_sdk_calls(&before_calls_config)
            .await
            .with_context(|| {
                format!(
                    "Failed to extract SDK calls from before_calls.go: {:?}",
                    before_calls
                )
            })?;

        // Extract SDK calls from create_function_calls.go
        let create_function_calls_config = ExtractSdkCallsConfig {
            source_files: vec![create_function_calls.clone()],
            language: Some("go".to_string()),
            service_hints: None,
        };
        let create_function_calls_methods =
            extract_sdk_calls::extract_sdk_calls(&create_function_calls_config)
                .await
                .with_context(|| {
                    format!(
                        "Failed to extract SDK calls from create_function_calls.go: {:?}",
                        create_function_calls
                    )
                })?;

        // Extract SDK calls from create_function_only.go
        let create_function_only_config = ExtractSdkCallsConfig {
            source_files: vec![create_function_only.clone()],
            language: Some("go".to_string()),
            service_hints: None,
        };
        let create_function_only_methods =
            extract_sdk_calls::extract_sdk_calls(&create_function_only_config)
                .await
                .with_context(|| {
                    format!(
                        "Failed to extract SDK calls from create_function_only.go: {:?}",
                        create_function_only
                    )
                })?;

        // Extract SDK calls from intermediate_calls.go
        let intermediate_calls_config = ExtractSdkCallsConfig {
            source_files: vec![intermediate_calls.clone()],
            language: Some("go".to_string()),
            service_hints: None,
        };
        let intermediate_calls_methods =
            extract_sdk_calls::extract_sdk_calls(&intermediate_calls_config)
                .await
                .with_context(|| {
                    format!(
                        "Failed to extract SDK calls from intermediate_calls.go: {:?}",
                        intermediate_calls
                    )
                })?;

        // Define output JSON file paths
        let after_calls_json_path = path.join("after_calls_extracted_sdk.json");
        let before_calls_json_path = path.join("before_calls_extracted_sdk.json");
        let create_function_calls_json_path = path.join("create_function_calls_extracted_sdk.json");
        let create_function_only_json_path = path.join("create_function_only_extracted_sdk.json");
        let intermediate_calls_json_path = path.join("intermediate_calls_extracted_sdk.json");

        // Check if any output files already exist
        let output_files = [
            &after_calls_json_path,
            &before_calls_json_path,
            &create_function_calls_json_path,
            &create_function_only_json_path,
            &intermediate_calls_json_path,
        ];

        let mut existing_files = Vec::new();
        for file_path in &output_files {
            if file_path.exists() {
                existing_files.push(file_path.display().to_string());
            }
        }

        if !existing_files.is_empty() {
            anyhow::bail!(
                "Output files already exist in directory {:?}. Please remove them first: {}",
                path,
                existing_files.join(", ")
            );
        }

        // Write extracted methods to JSON files using SdkMethodCall::serialize_list
        let full_output = true; // Include full metadata
        let pretty = true; // Pretty print JSON

        // Write after_calls_extracted_sdk.json
        let after_calls_json =
            SdkMethodCall::serialize_list(&after_calls_methods.methods, full_output, pretty)
                .context("Failed to serialize after_calls methods")?;
        fs::write(&after_calls_json_path, after_calls_json).with_context(|| {
            format!(
                "Failed to write after_calls JSON: {:?}",
                after_calls_json_path
            )
        })?;

        // Write before_calls_extracted_sdk.json
        let before_calls_json =
            SdkMethodCall::serialize_list(&before_calls_methods.methods, full_output, pretty)
                .context("Failed to serialize before_calls methods")?;
        fs::write(&before_calls_json_path, before_calls_json).with_context(|| {
            format!(
                "Failed to write before_calls JSON: {:?}",
                before_calls_json_path
            )
        })?;

        // Write create_function_calls_extracted_sdk.json
        let create_function_calls_json = SdkMethodCall::serialize_list(
            &create_function_calls_methods.methods,
            full_output,
            pretty,
        )
        .context("Failed to serialize create_function_calls methods")?;
        fs::write(&create_function_calls_json_path, create_function_calls_json).with_context(
            || {
                format!(
                    "Failed to write create_function_calls JSON: {:?}",
                    create_function_calls_json_path
                )
            },
        )?;

        // Write create_function_only_extracted_sdk.json
        let create_function_only_json = SdkMethodCall::serialize_list(
            &create_function_only_methods.methods,
            full_output,
            pretty,
        )
        .context("Failed to serialize create_function_only methods")?;
        fs::write(&create_function_only_json_path, create_function_only_json).with_context(
            || {
                format!(
                    "Failed to write create_function_only JSON: {:?}",
                    create_function_only_json_path
                )
            },
        )?;

        // Write intermediate_calls_extracted_sdk.json
        let intermediate_calls_json =
            SdkMethodCall::serialize_list(&intermediate_calls_methods.methods, full_output, pretty)
                .context("Failed to serialize intermediate_calls methods")?;
        fs::write(&intermediate_calls_json_path, intermediate_calls_json).with_context(|| {
            format!(
                "Failed to write intermediate_calls JSON: {:?}",
                intermediate_calls_json_path
            )
        })?;

        println!(
            "Extracted and wrote SDK calls for resource: {} (Service: {})",
            resource_name, metadata.service_dir_name
        );
    }

    println!("\nSDK call extraction completed");

    Ok(())
}

/// Analyze Terraform resources from extracted data
///
/// Iterates through subdirectories in the resource extractor output directory,
/// loads the 5 go files and metadata.json for each resource, and processes them.
///
/// # Arguments
///
/// * `resource_extractor_output` - Path to the directory containing resource subdirectories
/// * `_resource_schema_file` - Path to the resource schema file (currently unused)
/// * `_analysis_output_dir` - Path to the output directory for analysis results (currently unused)
///
/// # Returns
///
/// Returns `Ok(())` on success, or an error if directory reading or deserialization fails
pub async fn analyze_terraform_resources(
    resource_extractor_output: PathBuf,
    _resource_schema_file: PathBuf,
    analysis_output_dir: PathBuf,
) -> Result<()> {
    // Initialize the columns collections
    let mut columns = ResourceAnalysisColumns::new();
    let mut exploded_columns = ResourceAnalysisColumns::new();

    // Read all subdirectories in the resource_extractor_output directory
    let entries = fs::read_dir(&resource_extractor_output)
        .with_context(|| format!("Failed to read directory: {:?}", resource_extractor_output))?;

    for entry in entries {
        let entry = entry.with_context(|| "Failed to read directory entry")?;
        let path = entry.path();

        // Skip if not a directory
        if !path.is_dir() {
            continue;
        }

        // Extract resource name from directory name
        let resource_name = path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| anyhow::anyhow!("Invalid directory name: {:?}", path))?;

        // Build paths to the 5 go files
        let after_calls = path.join("after_calls.go");
        let before_calls = path.join("before_calls.go");
        let create_function_calls = path.join("create_function_calls.go");
        let create_function_only = path.join("create_function_only.go");
        let intermediate_calls = path.join("intermediate_calls.go");
        let metadata_path = path.join("metadata.json");

        //TODO: CHECK THAT FIRST/LAST ROW/COL IS NOT FUCKED UP (IT IS FOR, EG AWS_IAM_USER_GROUP_MEMBERSHIP)

        // Verify all files exist
        if !after_calls.exists()
            || !before_calls.exists()
            || !create_function_calls.exists()
            || !create_function_only.exists()
            || !intermediate_calls.exists()
            || !metadata_path.exists()
        {
            eprintln!(
                "Warning: Skipping directory {:?} - missing required files",
                path
            );
            continue;
        }

        // Load and deserialize metadata.json using serde_json
        let metadata_file = fs::File::open(&metadata_path)
            .with_context(|| format!("Failed to open metadata file: {:?}", metadata_path))?;

        let metadata: MetadataStruct = serde_json::from_reader(metadata_file)
            .with_context(|| format!("Failed to deserialize metadata JSON: {:?}", metadata_path))?;

        // Define paths to the extracted SDK call JSON files
        let after_calls_json_path = path.join("after_calls_extracted_sdk.json");
        let before_calls_json_path = path.join("before_calls_extracted_sdk.json");
        let create_function_calls_json_path = path.join("create_function_calls_extracted_sdk.json");
        let create_function_only_json_path = path.join("create_function_only_extracted_sdk.json");
        let intermediate_calls_json_path = path.join("intermediate_calls_extracted_sdk.json");

        // Verify all JSON files exist
        if !after_calls_json_path.exists()
            || !before_calls_json_path.exists()
            || !create_function_calls_json_path.exists()
            || !create_function_only_json_path.exists()
            || !intermediate_calls_json_path.exists()
        {
            eprintln!(
                "Warning: Skipping directory {:?} - missing required extracted SDK JSON files. Please run extract_terraform_resource_sdk_calls first.",
                path
            );
            continue;
        }

        // Read and deserialize after_calls_extracted_sdk.json
        let after_calls_json_file = fs::File::open(&after_calls_json_path).with_context(|| {
            format!(
                "Failed to open after_calls JSON: {:?}",
                after_calls_json_path
            )
        })?;
        let after_calls_methods: Vec<SdkMethodCall> =
            serde_json::from_reader(after_calls_json_file).with_context(|| {
                format!(
                    "Failed to deserialize after_calls JSON: {:?}",
                    after_calls_json_path
                )
            })?;

        // Read and deserialize before_calls_extracted_sdk.json
        let before_calls_json_file =
            fs::File::open(&before_calls_json_path).with_context(|| {
                format!(
                    "Failed to open before_calls JSON: {:?}",
                    before_calls_json_path
                )
            })?;
        let before_calls_methods: Vec<SdkMethodCall> =
            serde_json::from_reader(before_calls_json_file).with_context(|| {
                format!(
                    "Failed to deserialize before_calls JSON: {:?}",
                    before_calls_json_path
                )
            })?;

        // Read and deserialize create_function_calls_extracted_sdk.json
        let create_function_calls_json_file = fs::File::open(&create_function_calls_json_path)
            .with_context(|| {
                format!(
                    "Failed to open create_function_calls JSON: {:?}",
                    create_function_calls_json_path
                )
            })?;
        let create_function_calls_methods: Vec<SdkMethodCall> =
            serde_json::from_reader(create_function_calls_json_file).with_context(|| {
                format!(
                    "Failed to deserialize create_function_calls JSON: {:?}",
                    create_function_calls_json_path
                )
            })?;

        // Read and deserialize create_function_only_extracted_sdk.json
        let create_function_only_json_file = fs::File::open(&create_function_only_json_path)
            .with_context(|| {
                format!(
                    "Failed to open create_function_only JSON: {:?}",
                    create_function_only_json_path
                )
            })?;
        let create_function_only_methods: Vec<SdkMethodCall> =
            serde_json::from_reader(create_function_only_json_file).with_context(|| {
                format!(
                    "Failed to deserialize create_function_only JSON: {:?}",
                    create_function_only_json_path
                )
            })?;

        // Read and deserialize intermediate_calls_extracted_sdk.json
        let intermediate_calls_json_file = fs::File::open(&intermediate_calls_json_path)
            .with_context(|| {
                format!(
                    "Failed to open intermediate_calls JSON: {:?}",
                    intermediate_calls_json_path
                )
            })?;
        let intermediate_calls_methods: Vec<SdkMethodCall> =
            serde_json::from_reader(intermediate_calls_json_file).with_context(|| {
                format!(
                    "Failed to deserialize intermediate_calls JSON: {:?}",
                    intermediate_calls_json_path
                )
            })?;

        // Convert extracted methods to Vec<String>
        let before_calls_list: Vec<String> = before_calls_methods
            .iter()
            .map(|m| m.name.clone())
            .unique()
            .collect();
        let intermediate_calls_list: Vec<String> = intermediate_calls_methods
            .iter()
            .map(|m| m.name.clone())
            .unique()
            .collect();
        let after_calls_list: Vec<String> = after_calls_methods
            .iter()
            .map(|m| m.name.clone())
            .unique()
            .collect();
        let create_function_stack_calls_list: Vec<String> = create_function_calls_methods
            .iter()
            .map(|m| m.name.clone())
            .unique()
            .collect();
        let create_function_only_calls_list: Vec<String> = create_function_only_methods
            .iter()
            .map(|m| m.name.clone())
            .unique()
            .collect();

        // Create a row for this resource
        let row = ResourceAnalysisRow {
            service_name: metadata.service_dir_name.clone(),
            terraform_resource_name: metadata
                .terraform_resource_name.clone(),
            aws_sdk_resource_name: metadata
                .sdk_resource_name.clone(),
            num_before_sdk_calls: before_calls_list.len() as i32,
            before_sdk_calls: before_calls_list.join(", "),
            num_intermediate_sdk_calls: intermediate_calls_list.len() as i32,
            intermediate_sdk_calls: intermediate_calls_list.join(", "),
            num_after_sdk_calls: after_calls_list.len() as i32,
            after_sdk_calls: after_calls_list.join(", "),
            num_create_function_stack_calls: create_function_stack_calls_list.len() as i32,
            create_function_stack_calls: create_function_stack_calls_list.join(", "),
            num_create_function_only_calls: create_function_only_calls_list.len() as i32,
            create_function_only_calls: create_function_only_calls_list.join(", "),
            first_call_row: metadata.first_call_row,
            first_call_col: metadata.first_call_col,
            last_call_row: metadata.last_call_row,
            last_call_col: metadata.last_call_col,
            file_path: metadata.file_path.clone(),
        };

        // Append the row to the columns
        columns.append(row);

        // Create exploded rows - one for each create_function_only call
        if create_function_only_calls_list.is_empty() {
            // If no create_function_only calls, create one row with empty string
            let exploded_row = ResourceAnalysisRow {
                service_name: metadata.service_dir_name.clone(),
                terraform_resource_name: metadata
                    .terraform_resource_name.clone(),
                aws_sdk_resource_name: metadata
                    .sdk_resource_name.clone(),
                num_before_sdk_calls: before_calls_list.len() as i32,
                before_sdk_calls: before_calls_list.join(", "),
                num_intermediate_sdk_calls: intermediate_calls_list.len() as i32,
                intermediate_sdk_calls: intermediate_calls_list.join(", "),
                num_after_sdk_calls: after_calls_list.len() as i32,
                after_sdk_calls: after_calls_list.join(", "),
                num_create_function_stack_calls: create_function_stack_calls_list.len() as i32,
                create_function_stack_calls: create_function_stack_calls_list.join(", "),
                num_create_function_only_calls: 0,
                create_function_only_calls: String::new(),
                first_call_row: metadata.first_call_row,
                first_call_col: metadata.first_call_col,
                last_call_row: metadata.last_call_row,
                last_call_col: metadata.last_call_col,
                file_path: metadata.file_path.clone(),
            };
            exploded_columns.append(exploded_row);
        } else {
            // Create one row for each create_function_only call
            for sdk_call in &create_function_only_calls_list {
                let exploded_row = ResourceAnalysisRow {
                    service_name: metadata.service_dir_name.clone(),
                    terraform_resource_name: metadata
                        .terraform_resource_name.clone(),
                    aws_sdk_resource_name: metadata
                        .sdk_resource_name.clone(),
                    num_before_sdk_calls: before_calls_list.len() as i32,
                    before_sdk_calls: before_calls_list.join(", "),
                    num_intermediate_sdk_calls: intermediate_calls_list.len() as i32,
                    intermediate_sdk_calls: intermediate_calls_list.join(", "),
                    num_after_sdk_calls: after_calls_list.len() as i32,
                    after_sdk_calls: after_calls_list.join(", "),
                    num_create_function_stack_calls: create_function_stack_calls_list.len() as i32,
                    create_function_stack_calls: create_function_stack_calls_list.join(", "),
                    num_create_function_only_calls: create_function_only_calls_list.len() as i32,
                    create_function_only_calls: sdk_call.clone(),
                    first_call_row: metadata.first_call_row,
                    first_call_col: metadata.first_call_col,
                    last_call_row: metadata.last_call_row,
                    last_call_col: metadata.last_call_col,
                    file_path: metadata.file_path.clone(),
                };
                exploded_columns.append(exploded_row);
            }
        }

        println!(
            "Processed resource: {} (Service: {})",
            resource_name, metadata.service_dir_name
        );
    }

    // Create the DataFrame from the collected columns
    let df = columns
        .to_dataframe()
        .context("Failed to create DataFrame")?;

    println!("\nDataFrame created with {} rows", df.height());
    println!("DataFrame shape: {:?}", df.shape());

    // Create output directory if it doesn't exist
    fs::create_dir_all(&analysis_output_dir).with_context(|| {
        format!(
            "Failed to create output directory: {:?}",
            analysis_output_dir
        )
    })?;

    // Write DataFrame to CSV
    let csv_path = analysis_output_dir.join("terraform_resources_analysis.csv");
    let mut csv_file = std::fs::File::create(&csv_path)
        .with_context(|| format!("Failed to create CSV file: {:?}", csv_path))?;

    CsvWriter::new(&mut csv_file)
        .finish(&mut df.clone())
        .context("Failed to write DataFrame to CSV")?;

    println!("DataFrame written to CSV: {:?}", csv_path);

    // Create the exploded DataFrame from the collected columns
    let exploded_df = exploded_columns
        .to_dataframe()
        .context("Failed to create exploded DataFrame")?;

    println!(
        "\nExploded DataFrame created with {} rows",
        exploded_df.height()
    );
    println!("Exploded DataFrame shape: {:?}", exploded_df.shape());

    // Write exploded DataFrame to CSV
    let exploded_csv_path = analysis_output_dir.join("terraform_resources_analysis_exploded.csv");
    let mut exploded_csv_file = std::fs::File::create(&exploded_csv_path).with_context(|| {
        format!(
            "Failed to create exploded CSV file: {:?}",
            exploded_csv_path
        )
    })?;

    CsvWriter::new(&mut exploded_csv_file)
        .finish(&mut exploded_df.clone())
        .context("Failed to write exploded DataFrame to CSV")?;

    println!("Exploded DataFrame written to CSV: {:?}", exploded_csv_path);

    Ok(())
}
