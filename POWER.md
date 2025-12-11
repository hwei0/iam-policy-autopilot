---
name: "iam-policy-autopilot-power"
displayName: "IAM Policy Autopilot"
description: "AWS IAM Policy Autopilot analyzes your application code locally to generate identity-based policies for application roles, enabling faster IAM policy creation and reducing access troubleshooting time"
keywords: ["IAM", "AWS", "policy", "policies", "identity", "identity-based", "resource-based", "KMS", "S3", "resource", "permission", "boundary", "bucket", "sts", "access", "denial", "deny", "denied", "lambda", "cloudformation", "cfn", "cdk", "cloud", "analyzer"]
mcpServers: "iam-policy-autopilot-mcp"
---

# Onboarding

Before using IAM Policy Autopilot, make sure that the following are done and configured:
## Onboarding Part 1: validate that the binary command `iam-policy-autopilot` works, AND create necessary command aliases
This Kiro Power's MCP server requires the usage of the `iam-policy-autopilot` bin command. Users should have either: a) installed the `iam-policy-autopilot` binary through a direct installation or through pip; or b) installed the `uv` command line tool which gives them access to `uvx iam-policy-autopilot`. We must validate that either of them is true, and if (b) is true then we must install `iam-policy-autopilot` as a binary tool via `uv tool install iam-policy-autopilot` (our MCP server neeeds this). Accordingly, you MUST do the following steps:
1. try running `iam-policy-autopilot --version` in the CLI and checking that it does not return a "command not found" exception; it should return `iam-policy-autopilot: version-id`, where "version-id" is something like "0.1.0".
    - if running this `iam-policy-autopilot --version` command succeeds, then proceed to Onboarding Part 2, skipping the remainder of everything in this "Onboarding Part 1" section.
2. if the previous step failed, then try:
    - [step 2.1] running `uvx iam-policy-autopilot --version` in the CLI and checking that it does not return a "command not found" exception; it should return `iam-policy-autopilot: version-id`, where "version-id" is something like "0.1.0".
        - if running the `uvx iam-policy-autopilot --version` command succeeds, then go to step 2.2.
        - if running the `uvx iam-policy-autopilot --version` command fails, then the user has not configured the  prerequisites needed for this Kiro power. To fix this, go DIRECTLY to the "Remediation Steps" section below, and perform the remediation steps starting with "Remediation Step 1".
    - [step 2.2] if step 2.1 succeeds, then you MUST install the `iam-policy-autopilot` binary by running: `uv tool install iam-policy-autopilot`. 
        - if running the `uv tool install iam-policy-autopilot` command succeeds, then go to step 2.3. 
        - if running the `uv tool install iam-policy-autopilot` command fails, then the user has not configured the prerequisites needed for this Kiro power. To fix this, go DIRECTLY to the "Remediation Steps" section below, and perform the steps remediation starting with "Remediation Step 1".
    - [step 2.3] again try running `iam-policy-autopilot --version` in the CLI and checking that it does not return a "command not found" exception; it should return `iam-policy-autopilot: version-id`, where "version-id" is something like "0.1.0".
        - if running this `iam-policy-autopilot --version` command succeeds, then go DIRECTLY to Onboarding Part 2, skipping the remainder of everything in this "Onboarding Part 1" section.
        - if running this `iam-policy-autopilot --version` command fails, then the user has not configured the prerequisites needed for this Kiro power. To fix this, go DIRECTLY to the "Remediation Steps" section below, and perform the remediation steps starting with "Remediation Step 1".

### Remediation Steps: if the `iam-policy-autopilot` bin command does not exist, try to remediate this by doing the following remediation steps in order:
1. [Remediation Step 1] check that the user has the `pip` command installed; if so, then prompt the user to run `yes | pip install iam-policy-autopilot`. 
    - if the `yes | pip install iam-policy-autopilot` command succeeds, re-perform the call to `iam-policy-autopilot --version` and check that it succeeds. If it succeeds, then proceed directly to Onboarding Part 2 (i.e. skip the remaining Remediation steps in this list).
    - if the `yes | pip install iam-policy-autopilot` command fails, OR `pip` does not exist as a command, then try doing the next remediation step (Remediation Step 2) below.
2. [Remedation Step 2] try running a direct installation with this command: `curl -sSL https://github.com/awslabs/iam-policy-autopilot/raw/refs/heads/main/install.sh | sudo sh`
    - if the installation succeeds, re-perform the call to `iam-policy-autopilot --version` and check that it succeeds. If it succeeds, then go DIRECTLY to Onboarding Part 2 (i.e. skip the remaining Remediation steps in this list).
3. [Remediation Step 3] At this point, we cannot do any more remediation steps. FAIL THE ENTIRE KIRO POWER ONBOARDING PROCESS, and DO NOT PROCEED WITH THE SETUP OF THIS KIRO POWER. Tell the user to follow the setup guide in https://github.com/awslabs/iam-policy-autopilot/README.md for configuring the pre-requisites needed.

## Onboarding Part 2: validate that the `aws` bin command exists, and that AWS credentials are configured.
1. First, check that calling `aws --version` in the CLI does not return a "command not found" exception; it should instead return something like this: `aws-cli/2.27.18 Python/3.13.3 Darwin/25.1.0 exe/x86_64`.
    - if this does not work, FAIL THE ENTIRE KIRO POWER ONBOARDING PROCESS, and DO NOT PROCEED WITH THE SETUP OF THIS KIRO POWER. Tell the user to follow the setup guide for the AWS CLI in https://docs.aws.amazon.com/cli/v1/userguide/cli-chap-install.html.
2. Second, call `aws configure list` in the CLI returns a table like the following below, AND THAT the `access_key` and `secret_key` entries in the table have values that are set.
    - if this does not work, PROCEED with the kiro power onboarding process, but WARN the user that they need to configure aws credentials, by telling them to look at this link: https://docs.aws.amazon.com/cli/v1/userguide/cli-configure-files.html.
```
      Name                    Value             Type    Location
      ----                    -----             ----    --------
   profile                <not set>             None    None
access_key     ****************NIUM shared-credentials-file    
secret_key     ****************TYnY shared-credentials-file    
    region                us-west-2      config-file    ~/.aws/config
```

## If both onboarding parts 1 and 2 have succeeded, then onboarding is complete. If onboarding part 1 has succeeded but `aws configure list` in onboarding part 2 returned empty values for either the access key or secret key, then onboarding is complete, but warn the user that they must configure their aws credentials.


---
# Best Practices and Ideal Use Cases

To understand the best practices and use cases of this MCP server's tools, please read through ALL the instructions and use cases in the descriptions of the `generate_application_policies`, `generate_policy_for_access_denied`, and `fix_access_denied` tools in this IAM Policy Autopilot MCP server (`iam-policy-autopilot-mcp`). 

Specifically, there are certain cases when this MCP server excels:
- **generating IAM policies for a code file used in an AWS deployment (e.g. AWS Lambda function)**: the `generate_application_policies` tool in the `iam-policy-autopilot-mcp` MCP server does exactly this. Take a look through ALL the instructions and use cases for this tool, to better undstand how it is useful.
- **troubleshooting/resolving AWS IAM access denied errors**: the `generate_policy_for_access_denied` and `fix_access_denied` tools in the `iam-policy-autopilot-mcp` MCP servers can be used in combination to fix IAM access denied errors. Take a look through ALL the instructions and use cases for each of those tools, to better undstand how they are useful. For instance: if the user gives you an AWS access denied error they saw and asks you to diagnose/resolve it, OR if the user asks you to test an AWS deployment and you see an access denied error when testing, then you can do the following:
    1. invoke the `generate_policy_for_access_denied` tool, passing in the access denied error you saw. Follow ALL the instructions in that tool. This tool should retun an IAM policy to you, which should contain a fix for the access denied policy.
    2. Then call the `fix_access_denied` tool using both that IAM policy returned from the `generate_policy_for_access_denied` tool, as well as the original access denied error. This will deploy the IAM policy generated by the `generate_policy_for_access_denied` tool to the AWS account, in an attempt to fix the access denied error.