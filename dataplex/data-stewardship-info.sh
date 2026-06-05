#!/bin/bash
# This script creates a custom aspect type in Dataplex
# Before running: Replace 'your-project-id' with your actual GCP project ID
# Usage: bash data-stewardship-info.sh

ACCESS_TOKEN=$(gcloud auth print-access-token)

curl --request POST \
  "https://dataplex.googleapis.com/v1/projects/your-project-id/locations/eu/aspectTypes?aspectTypeId=data-stewardship-info" \
  --header "Authorization: Bearer $ACCESS_TOKEN" \
  --header 'Accept: application/json' \
  --header 'Content-Type: application/json' \
  --data '{
    "displayName": "Data Stewardship Information",
    "description": "Metadata related to data ownership and stewardship.",
    "metadataTemplate": {
      "name": "DataStewardshipTemplate",
      "type": "record",
      "recordFields": [
        {
          "name": "data_owner_email",
          "type": "string",
          "annotations": {
            "displayName": "Data Owner Email",
            "description": "Email address of the data owner."
          },
          "index": 1,
          "constraints": { "required": true }
        },
        {
          "name": "steward_team",
          "type": "string",
          "annotations": {
            "displayName": "Stewardship Team",
            "description": "Team responsible for data stewardship."
          },
          "index": 2
        },
        {
          "name": "last_reviewed_date",
          "type": "datetime",
          "annotations": {
            "displayName": "Last Reviewed Date",
            "description": "Date when the data asset was last reviewed for governance."
          },
          "index": 3
        }
      ]
    }
  }' \
  --compressed
