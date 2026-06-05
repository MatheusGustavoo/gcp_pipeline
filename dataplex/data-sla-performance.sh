#!/bin/bash
# This script creates a custom aspect type in Dataplex
# Before running: Replace 'your-project-id' with your actual GCP project ID
# Usage: bash data-sla-performance.sh

ACCESS_TOKEN=$(gcloud auth print-access-token)

curl --request POST \
  "https://dataplex.googleapis.com/v1/projects/your-project-id/locations/eu/aspectTypes?aspectTypeId=sla-and-performance" \
  --header "Authorization: Bearer $ACCESS_TOKEN" \
  --header 'Accept: application/json' \
  --header 'Content-Type: application/json' \
  --data '{
    "displayName": "SLA & Performance",
    "description": "Service level agreements and performance metrics.",
    "metadataTemplate": {
      "name": "SlaPerformanceTemplate",
      "type": "record",
      "recordFields": [
        {
          "name": "sla_uptime_percent",
          "type": "double",
          "annotations": {
            "displayName": "SLA Uptime %",
            "description": "Expected uptime percentage."
          },
          "index": 1
        },
        {
          "name": "max_latency_minutes",
          "type": "int",
          "annotations": {
            "displayName": "Max Latency (Minutes)",
            "description": "Maximum acceptable data refresh latency."
          },
          "index": 2
        },
        {
          "name": "support_contact",
          "type": "string",
          "annotations": {
            "displayName": "Support Contact",
            "description": "Who to contact for issues."
          },
          "index": 3
        }
      ]
    }
  }' \
  --compressed
