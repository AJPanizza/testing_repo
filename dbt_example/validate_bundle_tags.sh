#!/bin/bash

# This script validates that all jobs in a Databricks bundle have the required tags.
# It uses 'jq' to parse the JSON output of 'databricks bundle validate'.

set -euo pipefail

# --- Configuration ---
# Define the list of tags that are mandatory for every job.
readonly REQUIRED_TAGS=(
  "Business_Unit"
  "Team"
  "Product"
  "Project"
  "Budget_Code"
)

# --- Pre-flight Check ---
# Ensure the 'jq' command-line JSON processor is installed.
if ! command -v jq &> /dev/null; then
  echo "Error: 'jq' is not installed. Please install it to run this script." >&2
  exit 1
fi

# --- Main Logic ---
echo "Running 'databricks bundle validate' to get bundle resources..."
validation_output=$(databricks bundle validate --output json)

echo "Validating job tags..."

# Extract all job keys (names) from the bundle resources.
job_keys=$(echo "$validation_output" | jq -r '.resources.jobs | keys[]')

validation_failed=false

# Iterate over each job defined in the bundle.
for job_key in $job_keys; do
  missing_tags=()
  # For the current job, check for the existence of each required tag.
  for tag in "${REQUIRED_TAGS[@]}"; do
    # jq query to check if a specific tag key exists for the job.
    has_tag=$(echo "$validation_output" | jq --arg job_key "$job_key" --arg tag "$tag" \
      '.resources.jobs[$job_key].tags | has($tag)')

    if [[ "$has_tag" == "false" ]]; then
      missing_tags+=("$tag")
    fi
  done

  # If any tags are missing, report the error.
  if [ ${#missing_tags[@]} -ne 0 ]; then
    echo "ERROR: Job '$job_key' is missing required tags: ${missing_tags[*]}" >&2
    validation_failed=true
  fi
done

if $validation_failed; then
  echo -e "\nTag validation failed. Please add the missing tags to the job definitions." >&2
  exit 1
fi

echo "All jobs have the required tags. Validation successful!"
