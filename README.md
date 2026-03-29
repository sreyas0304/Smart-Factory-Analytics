# 🏭 Smart Factory Telemetry Data Architecture

![AWS](https://img.shields.io/badge/AWS-%23FF9900.svg?style=for-the-badge&logo=amazon-aws&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=Databricks&logoColor=white)
![Apache Flink](https://img.shields.io/badge/Apache_Flink-E6526F?style=for-the-badge&logo=apacheflink&logoColor=white)
![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)

## 📖 Executive Summary
This repository contains the Infrastructure as Code (IaC), streaming SQL, and Databricks data engineering pipelines for the Smart Factory Telemetry system at Tessolve Semiconductor Inc. 

[cite_start]The pipeline implements the industry-standard **Lambda Architecture** pattern [cite: 16][cite_start], built primarily on Amazon Web Services (AWS)[cite: 4]. It captures real-time IoT telemetry from manufacturing equipment (CNCs, Conveyors) and routes it through two parallel tracks:
* [cite_start]**Speed Layer (Hot Path):** Delivers sub-second operational insights to live web dashboards via Apache Flink and Amazon DynamoDB[cite: 6].
* [cite_start]**Batch Layer (Cold Path):** Archives immutable, clean telemetry data [cite: 7] into an S3 Data Lake, processed into a highly optimized Medallion Architecture using Databricks Lakehouse.

---

## 🏗️ Architecture Overview

![Smart Factory Architecture](./docs/data-pipeline.png)
*(Note: Ensure the architecture diagram is saved in the `/docs` folder to render here).*

### 🛠️ Core Technology Stack
* **Ingestion:** Python `boto3`, Amazon Kinesis Data Streams
* **Real-Time Stream Processing:** Amazon Managed Apache Flink (SQL), Amazon DynamoDB
* **Routing & Storage:** AWS Lambda, Amazon Kinesis Firehose, Amazon S3
* **Lakehouse Processing:** Databricks (Unity Catalog, Auto Loader, Delta Lake, CDF)
* [cite_start]**Resilience:** Amazon SQS (Dead Letter Queues) acting as the data lake's immune system[cite: 64], Slack Webhooks for workflow alerting.

---

## 🗂️ Repository Structure

This monorepo is organized by deployment lifecycle and separation of concerns:

```text
smart-factory-telemetry/
├── docs/                      # Architecture documentation and diagrams
├── src/
│   ├── generator/             # Python IoT telemetry simulator (ingest.py)
│   ├── hot_path_streaming/    # Flink SQL DDL and DML queries for live aggregations
│   ├── cold_path_routing/     # AWS Lambda Python deployment package
│   └── lakehouse_medallion/   # Databricks Asset Bundle containing Bronze/Silver/Gold pipelines
└── tests/                     # Unit tests for Lambda routing and Databricks DQ rules