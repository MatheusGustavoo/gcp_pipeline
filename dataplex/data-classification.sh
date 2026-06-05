#!/bin/bash
# This script creates a custom aspect type in Dataplex
# Before running: Replace 'your-project-id' with your actual GCP project ID
# Usage: bash data-classification.sh

ACCESS_TOKEN=$(gcloud auth print-access-token)

curl --request POST \
  "https://dataplex.googleapis.com/v1/projects/your-project-id/locations/eu/aspectTypes?aspectTypeId=data-classification" \
  --header "Authorization: Bearer $ACCESS_TOKEN" \
  --header 'Accept: application/json' \
  --header 'Content-Type: application/json' \
  --data '{
    "displayName": "Data Classification",
    "description": "Sensitivity and classification level of the data.",
    "metadataTemplate": {
      "name": "DataClassificationTemplate",
      "type": "record",
      "recordFields": [
        {
          "name": "sensitivity_level",
          "type": "string",
          "annotations": {
            "displayName": "Sensitivity Level",
            "description": "Public, Internal, Confidential, Restricted."
          },
          "index": 1,
          "constraints": { "required": true }
        },
        {
          "name": "business_unit",
          "type": "string",
          "annotations": {
            "displayName": "Business Unit",
            "description": "Which business unit owns this data."
          },
          "index": 2
        },
        {
          "name": "cost_center",
          "type": "string",
          "annotations": {
            "displayName": "Cost Center",
            "description": "Cost center for billing."
          },
          "index": 3
        }
      ]
    }
  }' \
  --compressed
