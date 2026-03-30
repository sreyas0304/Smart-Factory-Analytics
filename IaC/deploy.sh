#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

ENV="dev"
REGION="us-east-1" # Ensure this matches your targeted region

echo "Starting Smart Factory Infrastructure Deployment for environment: $ENV"

echo "Deploying Storage & State Layer..."
aws cloudformation deploy \
  --template-file 01-storage-state.yaml \
  --stack-name 01-storage-state-$ENV \
  --parameter-overrides Environment=$ENV \
  --region $REGION

echo "Deploying Streaming Core Layer..."
aws cloudformation deploy \
  --template-file 02-streaming-core.yaml \
  --stack-name 02-streaming-core-$ENV \
  --parameter-overrides Environment=$ENV \
  --region $REGION

echo "Deploying Cold Path Layer (Lambda & Firehose)..."
aws cloudformation deploy \
  --template-file 03-cold-path.yaml \
  --stack-name 03-cold-path-$ENV \
  --parameter-overrides Environment=$ENV \
  --capabilities CAPABILITY_NAMED_IAM \
  --region $REGION

echo "Deploying Hot Path Layer (Flink)..."
aws cloudformation deploy \
  --template-file 04-hot-path.yaml \
  --stack-name 04-hot-path-$ENV \
  --parameter-overrides Environment=$ENV \
  --capabilities CAPABILITY_NAMED_IAM \
  --region $REGION

echo "Deploying Analytics Layer (Glue & Athena)..."
aws cloudformation deploy \
  --template-file 05-analytics.yaml \
  --stack-name 05-analytics-$ENV \
  --parameter-overrides Environment=$ENV \
  --capabilities CAPABILITY_NAMED_IAM \
  --region $REGION
