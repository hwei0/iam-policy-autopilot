//! IAM Policy Autopilot Core API Interface

mod extract_sdk_calls;
mod generate_policies;
mod get_submodule_version;
mod iterate_operation_inputs;
mod iterate_service_references;
pub use extract_sdk_calls::extract_sdk_calls;
pub use generate_policies::generate_policies;
pub use get_submodule_version::{get_boto3_version_info, get_botocore_version_info};
pub use iterate_operation_inputs::{iterate_operation_inputs, InputMemberInfo};
pub use iterate_service_references::{
    iterate_service_references, AuthorizedActionInfo, OperationInfo,
};
/// analyze terraform resources
pub mod analyze_terraform_resources;
mod common;
pub mod model;
