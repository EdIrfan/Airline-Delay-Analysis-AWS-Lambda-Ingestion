# ✈️ Airline Delay Analysis — Serverless Ingestion & Orchestration

![AWS](https://img.shields.io/badge/AWS-%23FF9900.svg?style=flat&logo=amazon-aws&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.13-blue.svg?style=flat&logo=python&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621.svg?style=flat&logo=databricks&logoColor=white)
![AWS SAM](https://img.shields.io/badge/AWS_SAM-IaC-orange.svg?style=flat)
![Step Functions](https://img.shields.io/badge/Step_Functions-Orchestration-pink.svg?style=flat)
![Status](https://img.shields.io/badge/Status-Production-green.svg?style=flat)

> **This is one half of a two-repo system.**
> This repo handles all **AWS infrastructure, CI/CD, and orchestration**.
> The Databricks DLT transformation logic lives in the companion repo →
> [`Airline-Delay-Analysis-Databricks-Source-Files`](https://github.com/EdIrfan/Airline-Delay-Analysis-Databricks-Source-Files)

---

## 🧭 What Does This Repo Do?

This repository is the **trigger, orchestration, and deployment layer** of the Airline Delay Analysis pipeline. It doesn't transform data — it makes sure the right things happen at the right time, automatically, every time a new CSV lands in S3.

From a 6.5M-row CSV file upload to 7 fully-populated Gold analytics tables in Databricks — this repo owns everything in between on the AWS side.

**Zero manual intervention. Zero servers to manage.**

---

## 🏗️ Full System Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                        THIS REPO  (AWS Side)                         │
│                                                                      │
│  GitHub ──► CodeStar ──► CodePipeline ──► CodeBuild ──► CFN         │
│                                                    └──► Lambda x4   │
│                                                                      │
│  CSV Upload ──► S3 Landing Bucket                                    │
│                    └──► EventBridge (Object Created)                 │
│                              └──► Step Functions State Machine       │
│                                      ├── Launcher Lambda             │
│                                      ├── Wait (60s polling)          │
│                                      ├── Checker Lambda              │
│                                      └── Cleanup Lambda              │
│                                           └──► [on failure]          │
│                                                 Error Handler Lambda │
│                                                 └──► SQS ──► SNS ──► Email │
└──────────────────────────────────────────────────────────────────────┘
                              │
                              │  triggers Databricks Job via REST API
                              ▼
┌──────────────────────────────────────────────────────────────────────┐
│                  COMPANION REPO  (Databricks Side)                   │
│   Bronze (Auto Loader) → Silver (11 quality rules) → Gold (7 tables)│
└──────────────────────────────────────────────────────────────────────┘
```

---

## 🔗 Dependency on Companion Repo

This repo **cannot function without** the Databricks pipeline defined in:
**[`Airline-Delay-Analysis-Databricks-Source-Files`](https://github.com/EdIrfan/Airline-Delay-Analysis-Databricks-Source-Files)**

| This Repo Does | Companion Repo Does |
|---|---|
| Detects CSV upload via EventBridge | Reads CSV from S3 via Auto Loader (Bronze) |
| Authenticates with Databricks via Secrets Manager | Runs DLT transformations (Silver) |
| Triggers the Databricks Job via REST API | Applies 11 quality rules & builds 7 Gold tables |
| Polls job status every 60s (Checker Lambda) | Outputs analytics-ready Delta tables |
| Deletes S3 file on success (Cleanup Lambda) | Handles full refresh on every run |
| Sends email alert on failure (Error Handler) | — |

The `DatabricksJobId` and `DatabricksPipelineId` in `samconfig.toml` and `parameters_*.json` are the bridge between both repos.

---

## ⚙️ Step Functions State Machine

The core of this repo is the AWS Step Functions state machine that executes on every CSV upload:

```
TriggerDatabricks  (Launcher λ)
    │  fetches PAT from Secrets Manager
    │  PUTs updated config to Databricks Pipelines API
    │  POSTs run-now to Databricks Jobs API  →  returns run_id
    ▼
WaitForProcessing  (60s wait state)
    ▼
CheckStatus  (Checker λ)
    │  GETs /api/2.1/jobs/runs/get with run_id
    │  returns  RUNNING | SUCCESS | FAILED
    ▼
EvaluateStatus  (Choice state)
    ├── SUCCESS ──► RunCleanup  (Cleanup λ — deletes the S3 source file)
    ├── RUNNING ──► back to WaitForProcessing  (loops until done)
    └── DEFAULT ──► ReportFailure  (Error Handler λ → SQS → SNS → Email)
```

---

## 🚀 CI/CD Pipeline — Self-Mutating Infrastructure

Every push to the tracked branch triggers a full deploy automatically. No manual `sam deploy` ever needed.

```
GitHub Push
    └──► CodeStar Connection  (GitHub App auth)
              └──► CodePipeline triggered
                        │
                        ├── BUILD  (CodeBuild)
                        │     ├── Spins up Python 3.13 environment
                        │     ├── pip install all dependencies
                        │     ├── pytest — all unit tests must pass
                        │     ├── sam build  (resolves template.yml)
                        │     └── sam package  (artifacts → S3 bucket)
                        │
                        └── DEPLOY  (CloudFormation)
                              ├── Action 1: Create ChangeSet  (validate diff)
                              └── Action 2: Execute ChangeSet  (deploy)
                                    └── All 4 Lambdas updated, zero downtime
```

The infrastructure is **self-mutating** — the pipeline can update its own CodePipeline, Lambdas, and IAM roles on every merge.

---

## 📂 Repository Structure

```
Airline-Delay-Analysis-AWS-Lambda-Ingestion/
│
├── CDF_Template/
│   ├── template.yml              # AWS SAM template — all infrastructure defined here
│   ├── parameters_dev.json       # Dev environment overrides (Databricks IDs, ARNs)
│   └── parameters_prod.json      # Prod environment overrides
│
├── src/
│   ├── launcher/                 # Triggers the Databricks job (pipeline entry point)
│   ├── checker/                  # Polls Databricks job status every 60s
│   ├── cleanup/                  # Deletes S3 source file on success (idempotent)
│   └── error_handler/            # Formats + routes failure alerts via SQS → SNS
│
├── test/
│   └── unit/                     # pytest unit tests for all 4 Lambda functions
│
├── buildspec.yml                 # CodeBuild phases: install → test → build → package
├── samconfig.toml                # SAM deploy config (stack name, region, param overrides)
├── requirements.txt              # Shared Lambda dependencies
└── README.md
```

---

## 🔧 Key Configuration

All environment-specific values live in `parameters_*.json` and `samconfig.toml`:

| Parameter | Description |
|---|---|
| `EnvType` | Deployment environment (`dev` / `prod`) |
| `DatabricksHost` | Databricks workspace URL |
| `DatabricksJobId` | Job ID to trigger in the companion repo |
| `DatabricksPipelineId` | DLT Pipeline ID to update config on before each run |
| `SecretArn` | ARN of the Secrets Manager secret holding the Databricks PAT |
| `NotificationEmail` | Email address for failure alerts via SNS |
| `ArtifactBucket` | S3 bucket for SAM deployment artifacts |

---

## 🛡️ Security

- **No hardcoded credentials.** The Databricks PAT is stored in AWS Secrets Manager and fetched at Lambda runtime — never in environment variables or code.
- **IAM scoped roles.** All 4 Lambdas share a `LambdaExecutionRole` with a `LambdaAccessPolicy` covering only: Secrets Manager read, S3 get/delete, SQS send/receive/delete, SNS publish.
- **Note:** This is a personal project — the CloudFormation execution role has broader permissions than strict least-privilege would require. Production hardening would scope these down further.

---

## 🧪 Running Tests Locally

```bash
# From repo root
pip install -r requirements.txt
pip install -r test/requirements.txt
export PYTHONPATH=$PYTHONPATH:$(pwd)/src
python -m pytest test/unit/ -v
```

---

## 📊 Pipeline Stats

| Metric | Value |
|---|---|
| Records processed annually | ~6.5 M |
| Records filtered by quality rules | ~700 K |
| Gold analytics tables produced | 7 |
| Lambda functions deployed | 4 |
| Quality rules (companion repo) | 11 |
| Estimated Lambda cost | $1–2 / month |
| Manual steps required | **0** |

---

## 🤝 Related

- **Companion repo (DLT transformations):** [`Airline-Delay-Analysis-Databricks-Source-Files`](https://github.com/EdIrfan/Airline-Delay-Analysis-Databricks-Source-Files)
- **AWS Stack name:** `Airline-App-Stack-prod`
- **Region:** `us-east-1`