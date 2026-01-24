use anyhow::{Context, Result};
use std::path::PathBuf;

use crate::context_fetcher::{terraform_state::TerraformStateContext, TerraformProjectExplorer};

/// get the terraform state.
pub async fn get_terraform_state(terraform_dir: PathBuf) -> Result<(TerraformStateContext)> {
    let terraform_context = TerraformProjectExplorer::new(&terraform_dir)?;

    Ok(terraform_context.terraform_state_context)
}
