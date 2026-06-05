#!/bin/bash
# This script creates a custom aspect type in Dataplex
# Before running: Replace 'your-project-id' with your actual GCP project ID
# Usage: bash data-freshness.sh

ACCESS_TOKEN=$(gcloud auth print-access-token)

curl --request POST \
  "https://dataplex.googleapis.com/v1/projects/your-project-id/locations/eu/aspectTypes?aspectTypeId=data-freshness" \
  --header "Authorization: Bearer $ACCESS_TOKEN" \
  --header 'Accept: application/json' \
  --header 'Content-Type: application/json' \
  --data '{
    "displayName": "Data Freshness",
    "description": "Metadata related to data freshness and update frequency.",
    "metadataTemplate": {
      "name": "DataFreshnessTemplate",
      "type": "record",
      "recordFields": [
          {
          "name": "last_updated_timestamp",
          "type": "datetime",
          "annotations": {
              "displayName": "Last Updated Timestamp",
              "description": "The timestamp of the last data update."
          },
          "index": 1
          },
          {
          "name": "update_frequency",
          "type": "string",
          "annotations": {
              "displayName": "Update Frequency",
              "description": "How often the data is updated (e.g., Daily, Hourly, Weekly)."
          },
          "index": 2
          },
          {
          "name": "expected_next_update",
          "type": "datetime",
          "annotations": {
              "displayName": "Expected Next Update",
              "description": "The expected timestamp of the next data update."
          },
          "index": 3
          }
      ]
    }
  }' \
  --compressed
