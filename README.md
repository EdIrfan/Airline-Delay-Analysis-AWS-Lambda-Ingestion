# ✈️ Airline Delay Analysis - Serverless Ingestion & Orchestration Pipeline

![AWS](https://img.shields.io/badge/AWS-%23FF9900.svg?style=flat&logo=amazon-aws&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.13-blue.svg?style=flat&logo=python&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-F3722C.svg?style=flat&logo=databricks&logoColor=white)
![AWS SAM](https://img.shields.io/badge/AWS_SAM-Serverless-red.svg?style=flat&logo=serverless&logoColor=white)

## 📌 Project Overview
This repository contains the infrastructure and application code for a fully automated, serverless data ingestion pipeline. Built natively on AWS using the Serverless Application Model (SAM), this architecture orchestrates data workflows between AWS S3 and Databricks, providing a highly scalable and fault-tolerant foundation for airline delay analysis.

The project features a self-mutating CI/CD pipeline leveraging AWS CodePipeline and CodeBuild, ensuring that all infrastructure and application updates are continuously integrated, tested, and deployed with zero manual intervention.

---

## 🏗️ Architecture & Core Components
The backend logic is decoupled into three distinct AWS Lambda functions, ensuring modularity and adherence to the single-responsibility principle:

### 1. Launcher Function (`src/launcher/`)
* **Role:** The entry point of the pipeline.
* **Function:** Ingests trigger events, authenticates with external APIs, and initiates the downstream Databricks workflow (`DatabricksPipelineId`).

### 2. Checker Function (`src/checker/`)
* **Role:** State monitor.
* **Function:** Asynchronously polls the status of the running Databricks jobs, handling timeouts, pipeline failures, and successful completion states.

### 3. Cleanup Function (`src/cleanup/`)
* **Role:** Lifecycle management.
* **Function:** Executes post-processing tasks, including the archival or deletion of processed S3 objects and parameter sanitization, ensuring storage optimization.

---

## 🚀 CI/CD Pipeline Lifecycle
The deployment lifecycle is entirely automated via AWS CodePipeline, utilizing a strict two-step CloudFormation Change Set pattern to ensure safe infrastructure updates.

* **Source Stage:** Connects directly to the repository via a secure GitHub App (CodeStar Connection). Listens for merges to the `dev` or `prod` branches.
* **Build Stage (AWS CodeBuild):**
  * Provisions a Python 3.13 environment.
  * Executes the `buildspec.yml`.
  * Runs the automated test suite (`pytest`) against all Lambda functions.
  * Uses the AWS SAM CLI to resolve dependencies, compile the application, and package artifacts into the designated S3 Artifact Bucket.
  * Dynamically injects environment-specific parameters (`parameters_dev.json` / `parameters_prod.json`).
* **Deploy Stage (AWS CloudFormation):**
  * **Action 1:** Generates a CloudFormation Change Set, validating the updated SAM template and JSON parameters against the live AWS environment.
  * **Action 2:** Executes the Change Set, seamlessly updating IAM roles, Lambdas, and API configurations with zero downtime.

---

## 📂 Repository Structure
```text
.
├── CDF_Template/
│   ├── template.yml              # Primary AWS SAM Infrastructure-as-Code template
│   ├── parameters_dev.json       # Development environment variables (e.g., Databricks IDs)
│   └── parameters_prod.json      # Production environment variables
├── src/
│   ├── launcher/                 # Launcher Lambda source code & requirements
│   ├── checker/                  # Checker Lambda source code & requirements
│   └── cleanup/                  # Cleanup Lambda source code & requirements
├── test/
│   ├── unit/                     # Pytest unit tests for all Lambda functions
│   └── requirements.txt          # Testing dependencies
├── buildspec.yml                 # CodeBuild execution instructions
└── README.md
```

---

## ⚙️ Deployment & Configuration
Because this architecture is managed via CodePipeline, manual terminal deployments are disabled for standard workflow progression.

* **Environment Variables**
Pipeline configuration is controlled via the parameters_${EnvType}.json files. Ensure the following keys are correctly defined before merging to the target branch:

* **EnvType:** The deployment environment (e.g., dev, prod).

* **DatabricksPipelineId:** The unique identifier for the target Databricks Delta Live Tables (DLT) or job pipeline.

* **ArtifactBucket:** The S3 bucket designated for SAM deployment packages.

* **Triggering a Deployment:**
Create a feature branch and implement code or infrastructure updates.

* Ensure all tests pass locally.

* Open a Pull Request and merge into the tracked branch (e.g., dev).

* AWS CodePipeline will automatically intercept the webhook, build the SAM template, and deploy the updated CloudFormation stack.

---

## 🛡️ Security & IAM
* **Privileges:** The pipeline utilizes a dedicated CloudFormationExecutionRole for assigning Roles, but since this is a demo project, I have not followed Principle of Least Priviledge and went on to create this with more priviledges than required.

* **Secret Management:** Sensitive credentials (like Databricks API tokens) are not hardcoded. They are dynamically resolved at runtime via AWS Secrets Manager.