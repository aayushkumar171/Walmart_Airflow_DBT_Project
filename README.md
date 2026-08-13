# Walmart Data Engineering Pipeline --- Airflow + dbt + Databricks

![Architecture](docs/architecture/architecture.png)

An end-to-end data engineering pipeline designed to ingest
Walmart-related operational data from multiple sources, process
incremental changes, orchestrate transformations with Apache Airflow,
transform and test data with dbt, and prepare analytics-ready datasets
on Databricks.

The project demonstrates a production-oriented ELT architecture with
**CDC, file ingestion, incremental processing, data quality checks,
business transformations, dimensional modeling, and orchestration**.

------------------------------------------------------------------------

## Table of Contents

-   [Project Overview](#project-overview)
-   [Architecture](#architecture)
-   [Architecture Components](#architecture-components)
-   [End-to-End Data Flow](#end-to-end-data-flow)
-   [1. Source Systems](#1-source-systems)
-   [2. Change Data Capture](#2-change-data-capture)
-   [3. File-Based Ingestion](#3-file-based-ingestion)
-   [4. Incremental Processing](#4-incremental-processing)
-   [5. Apache Airflow](#5-apache-airflow)
-   [6. dbt Transformation Layer](#6-dbt-transformation-layer)
-   [7. One-Big-Table / Business
    Layer](#7-one-big-table--business-layer)
-   [8. Data Quality](#8-data-quality)
-   [9. Gold / Dimensional Modeling](#9-gold--dimensional-modeling)
-   [10. Databricks and Delta Lake](#10-databricks-and-delta-lake)
-   [Project Structure](#project-structure)
-   [Technology Stack](#technology-stack)
-   [Data Modeling](#data-modeling)
-   [Incremental Processing](#incremental-processing-1)
-   [Snapshots and Historical
    Tracking](#snapshots-and-historical-tracking)
-   [Data Quality Strategy](#data-quality-strategy)
-   [Orchestration Flow](#orchestration-flow)
-   [How to Run the Project](#how-to-run-the-project)
-   [Environment Variables](#environment-variables)
-   [Example Pipeline Execution](#example-pipeline-execution)
-   [Engineering Concepts
    Demonstrated](#engineering-concepts-demonstrated)
-   [Future Improvements](#future-improvements)
-   [Author](#author)

------------------------------------------------------------------------

## Project Overview

This project implements a modern data engineering workflow for
transforming operational Walmart data into analytics-ready datasets.

The architecture supports two primary ingestion paths:

1.  **Agentic DB → CDC → Databricks**
2.  **AWS S3 → Files → Databricks**

The data is then processed through an orchestration and transformation
pipeline:

``` text
Agentic DB ── CDC ──────────────┐
                                │
AWS S3 ── Files ────────────────┤
                                ▼
                         Databricks
                                │
                                ▼
                         Incremental Layer
                                │
                                ▼
                         Apache Airflow
                                │
                                ▼
                              dbt
                                │
                    ┌───────────┴───────────┐
                    ▼                       ▼
             Business / OBT          Gold Models
                    │                       │
                    └───────────┬───────────┘
                                ▼
                         Quality Checks
                                │
                                ▼
                       Analytics-Ready Data
```

The objective is not simply to move data from one system to another. The
project focuses on **reliable, repeatable, testable, and maintainable
data pipelines**.

------------------------------------------------------------------------

# Architecture

The architecture below represents the complete pipeline.

![Walmart Data Engineering
Architecture](docs/architecture/architecture.png)

### High-level flow

``` text
                   ┌──────────────────┐
                   │    Agentic DB    │
                   │  PostgreSQL DB   │
                   └────────┬─────────┘
                            │
                           CDC
                            │
                            ▼
                   ┌──────────────────┐
                   │                  │
                   │    Databricks    │
                   │                  │
                   └────────┬─────────┘
                            │
                            │
AWS S3 ─────── Files ───────┤
                            │
                            ▼
                   ┌──────────────────┐
                   │   Incremental    │
                   │    Processing    │
                   └────────┬─────────┘
                            │
                            ▼
                   ┌──────────────────┐
                   │  Apache Airflow  │
                   │  Orchestration   │
                   └────────┬─────────┘
                            │
                            ▼
                   ┌──────────────────┐
                   │       dbt        │
                   │ Transformation   │
                   └────────┬─────────┘
                            │
               ┌────────────┴────────────┐
               ▼                         ▼
        One-Big-Table              Gold Models
               │                         │
               └────────────┬────────────┘
                            ▼
                    Quality Checks
                            │
                            ▼
                    Analytics Layer
```

------------------------------------------------------------------------

# Architecture Components

## 1. Source Systems

The pipeline receives data from two sources.

### Agentic Database

The database acts as an operational source containing business entities
such as:

-   Customers
-   Employees
-   Orders
-   Order items
-   Products
-   Stores

The database is connected to the pipeline through a **Change Data
Capture (CDC)** mechanism.

### AWS S3

AWS S3 provides a file-based ingestion path.

Files arriving in S3 can be processed by the pipeline without requiring
a direct database connection.

This makes the architecture capable of handling both:

-   database-driven ingestion
-   file-driven ingestion

------------------------------------------------------------------------

# 2. Change Data Capture

The database ingestion path uses **CDC**.

Instead of repeatedly copying the entire source database, CDC captures
changes such as:

``` text
INSERT
UPDATE
DELETE
```

This is useful because production databases can contain millions of
records.

A full reload would require repeatedly processing all records:

``` text
Source Table
     │
     ▼
Read Everything
     │
     ▼
Transform Everything
     │
     ▼
Write Everything
```

CDC instead focuses on changed records:

``` text
Source Table
     │
     ▼
Detect Changes
     │
     ├── INSERT
     ├── UPDATE
     └── DELETE
          │
          ▼
    Process Changes
```

### Benefits

-   Reduced processing cost
-   Lower data movement
-   Faster pipelines
-   Better scalability
-   Near-real-time or frequent incremental ingestion
-   Better support for large source systems

------------------------------------------------------------------------

# 3. File-Based Ingestion

The second ingestion path starts from AWS S3.

``` text
AWS S3
  │
  └── Files
        │
        ▼
   Databricks
        │
        ▼
Incremental Processing
```

This path is useful when source systems deliver:

-   CSV files
-   JSON files
-   Parquet files
-   periodic exports
-   batch files

The architecture therefore supports more than one source ingestion
pattern.

------------------------------------------------------------------------

# 4. Incremental Processing

After ingestion, data is processed incrementally.

Incremental processing means that the pipeline does not unnecessarily
rebuild the complete dataset during every execution.

Conceptually:

``` text
Previous Data
     +
New / Changed Data
     │
     ▼
Incremental Transformation
     │
     ▼
Updated Dataset
```

This is particularly important for:

-   Orders
-   Customers
-   Products
-   Store data
-   Large transactional tables

The incremental layer acts as a bridge between ingestion and downstream
dbt transformations.

------------------------------------------------------------------------

# 5. Apache Airflow

Apache Airflow is the **orchestration layer** of the project.

Airflow is responsible for controlling when pipeline tasks run and how
they depend on each other.

A simplified workflow is:

``` text
Ingestion
   │
   ▼
Incremental Processing
   │
   ▼
Source Validation
   │
   ▼
dbt Transformations
   │
   ▼
Data Quality Tests
   │
   ▼
Gold Models
```

Airflow provides:

-   Scheduling
-   Task dependency management
-   Retries
-   Logging
-   Monitoring
-   Failure visibility
-   Workflow management

### Why Airflow?

The transformation logic and orchestration logic have different
responsibilities.

**dbt** answers:

> How should the data be transformed?

**Airflow** answers:

> When should the transformations run, and in what order?

Keeping those responsibilities separate makes the pipeline easier to
maintain.

------------------------------------------------------------------------

# 6. dbt Transformation Layer

dbt is used for the SQL-based transformation layer.

The dbt project follows a layered transformation strategy.

``` text
Source
   │
   ▼
Silver Technical
   │
   ▼
Silver Business
   │
   ▼
Gold
```

### Silver Technical Layer

This layer focuses on technical transformations such as:

-   Cleaning
-   Standardization
-   Type conversion
-   Deduplication
-   Incremental processing
-   Basic business-safe transformations

Typical models include:

``` text
customers_t
employees_t
orders_t
order_items_t
products_t
stores_t
```

The `_t` naming represents transformed technical datasets.

------------------------------------------------------------------------

# 7. One-Big-Table / Business Layer

The business layer creates a consolidated dataset that can be used for
downstream analytical processing.

This layer can combine information from multiple technical models.

Conceptually:

``` text
Customers
     │
     ├─────────────┐
Products           │
     │             │
Orders             ├──► Business OBT
     │             │
Order Items        │
     │             │
Stores ────────────┘
```

The purpose of an OBT / business-level model is to provide a convenient
representation of business data for downstream consumption.

This avoids repeatedly rebuilding the same complex joins for every
analytical use case.

------------------------------------------------------------------------

# 8. Data Quality

Data quality is treated as a first-class part of the pipeline.

The project uses dbt tests and validation checks to detect problems
before data reaches the final analytical layer.

Typical checks include:

### Not Null

Ensures important fields contain values.

``` text
customer_id IS NOT NULL
```

### Unique

Ensures identifiers do not appear multiple times when uniqueness is
expected.

``` text
order_id → UNIQUE
```

### Relationships

Validates foreign-key-like relationships.

``` text
fact_orders.customer_id
          │
          ▼
dim_customers.customer_id
```

### Source Freshness

Checks whether source data is arriving within the expected freshness
window.

### Why quality checks matter

Without quality checks:

``` text
Bad Data
   │
   ▼
Transformation
   │
   ▼
Gold Table
   │
   ▼
Dashboard
   │
   ▼
Wrong Business Decision
```

With validation:

``` text
Raw Data
   │
   ▼
Quality Checks
   │
   ├── PASS ──► Transform
   │
   └── FAIL ──► Stop / Investigate
```

------------------------------------------------------------------------

# 9. Gold / Dimensional Modeling

The final analytical layer follows a dimensional modeling approach.

The project contains dimensions and fact-oriented models.

Typical dimensions include:

``` text
dim_customers
dim_employees
dim_orders
dim_products
dim_stores
```

The main transactional fact model is:

``` text
fact_orders
```

A simplified star schema is:

``` text
                    dim_customers
                         │
                         │
                         ▼
dim_products ─────── fact_orders ─────── dim_stores
                         │
                         │
                         ▼
                    dim_employees
                         │
                         ▼
                     dim_orders
```

### Fact table

The fact table represents measurable business events.

For example:

-   Orders
-   Quantities
-   Revenue
-   Product activity
-   Customer activity

### Dimension tables

Dimensions provide descriptive context around facts.

Examples:

-   Customer information
-   Product information
-   Store information
-   Employee information

This structure makes analytical queries easier and more efficient.

------------------------------------------------------------------------

# 10. Databricks and Delta Lake

Databricks provides the processing environment for the data engineering
workflow.

The architecture uses Databricks to handle:

-   Ingestion
-   Incremental processing
-   Distributed data processing
-   Data storage/management
-   Delta-based data operations

Delta Lake provides additional capabilities over raw file storage,
including:

-   ACID transactions
-   Schema enforcement
-   Schema evolution
-   Time travel
-   Reliable updates
-   Better data management

Conceptually:

``` text
S3 / Database
      │
      ▼
 Databricks
      │
      ▼
 Delta-based Data
      │
      ▼
 dbt Transformations
```

------------------------------------------------------------------------

# Project Structure

A recommended repository structure is:

``` text
walmart-airflow-dbt/
│
├── README.md
├── LICENSE
├── .gitignore
├── .env.example
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
│
├── dags/
│   └── orchestrate.py
│
├── dbt/
│   └── walmart_data_engineer/
│       ├── dbt_project.yml
│       ├── profiles.yml.example
│       │
│       ├── models/
│       │   ├── source/
│       │   ├── silver_t/
│       │   ├── silver_b/
│       │   └── gold/
│       │
│       ├── snapshots/
│       ├── tests/
│       ├── macros/
│       ├── seeds/
│       └── analyses/
│
├── docs/
│   ├── architecture/
│   │   └── architecture.png
│   ├── airflow/
│   ├── dbt/
│   └── screenshots/
│
└── data/
    └── sample/
```

Generated runtime files such as `logs/`, `target/`, `__pycache__/`, and
local environment files should not be committed to Git.

------------------------------------------------------------------------

# Technology Stack

  Technology       Purpose
  ---------------- ---------------------------------
  Python           Pipeline/orchestration support
  Apache Airflow   Workflow orchestration
  dbt              SQL transformation and testing
  Databricks       Data processing platform
  Delta Lake       Reliable data storage layer
  AWS S3           File-based source
  PostgreSQL       Operational/source database
  Docker           Local containerized environment
  Git/GitHub       Version control
  SQL              Data transformation

------------------------------------------------------------------------

# Data Modeling

The project uses a layered data architecture.

## Source Layer

Raw operational data.

``` text
Agentic DB
AWS S3
```

## Incremental Layer

Only new or changed data is processed.

``` text
CDC / Files
     │
     ▼
Incremental Dataset
```

## Silver Layer

Cleaned and standardized data.

``` text
customers_t
employees_t
orders_t
order_items_t
products_t
stores_t
```

## Business Layer

Business-ready joined representation.

``` text
obt_b
```

## Gold Layer

Analytics-ready dimensional models.

``` text
Dimensions
    +
Facts
```

------------------------------------------------------------------------

# Incremental Processing

Incremental processing is one of the main engineering concepts
demonstrated by the project.

A simplified incremental strategy is:

``` text
Existing Dataset
      +
Incoming Records
      │
      ▼
Identify New / Changed Rows
      │
      ▼
Transform
      │
      ▼
Merge / Append
      │
      ▼
Updated Dataset
```

The main advantage is that processing cost grows with the amount of
changed data rather than always growing with the entire source table.

------------------------------------------------------------------------

# Snapshots and Historical Tracking

The project uses dbt snapshots for historical tracking.

Snapshots are useful when the business needs to answer questions such
as:

> What did this customer/product/store record look like before it
> changed?

Instead of simply overwriting:

``` text
Customer
   │
   └── Update
          │
          ▼
      New Value
```

historical tracking can preserve versions:

``` text
Customer ID
     │
     ├── Version 1
     │      Valid From
     │      Valid To
     │
     └── Version 2
            Valid From
            Valid To
```

This is commonly associated with **Slowly Changing Dimension Type 2 (SCD
Type 2)** patterns.

The project applies this concept to dimensional data where historical
changes are important.

------------------------------------------------------------------------

# Data Quality Strategy

The quality strategy has multiple layers.

``` text
Source Freshness
       │
       ▼
Schema / Column Validation
       │
       ▼
dbt Tests
       │
       ▼
Relationship Checks
       │
       ▼
Gold Data
```

Important quality checks include:

-   `not_null`
-   `unique`
-   relationship validation
-   source freshness
-   custom SQL tests where required

The goal is to prevent invalid records from silently propagating
downstream.

------------------------------------------------------------------------

# Orchestration Flow

The pipeline is designed to run in a controlled sequence.

A representative execution flow is:

``` text
             ┌─────────────────┐
             │ Source / Ingest │
             └────────┬────────┘
                      ▼
             ┌─────────────────┐
             │   Incremental   │
             │    Processing   │
             └────────┬────────┘
                      ▼
             ┌─────────────────┐
             │ Source Freshness│
             │     Checks      │
             └────────┬────────┘
                      ▼
             ┌─────────────────┐
             │ Silver Technical│
             └────────┬────────┘
                      ▼
             ┌─────────────────┐
             │ Silver Tests    │
             └────────┬────────┘
                      ▼
             ┌─────────────────┐
             │ Silver Business │
             └────────┬────────┘
                      ▼
             ┌─────────────────┐
             │ Business Tests  │
             └────────┬────────┘
                      ▼
             ┌─────────────────┐
             │ Gold / Ephemeral│
             └────────┬────────┘
                      ▼
             ┌─────────────────┐
             │ Gold Snapshots  │
             └────────┬────────┘
                      ▼
             ┌─────────────────┐
             │ Gold Facts      │
             └─────────────────┘
```

This dependency-driven design ensures that downstream models do not run
before their upstream dependencies are ready.

------------------------------------------------------------------------

# How to Run the Project

## Prerequisites

Install:

-   Docker Desktop
-   Git
-   Python 3.x
-   A Databricks workspace
-   Access to the required source data
-   AWS S3 access if using the S3 ingestion path

------------------------------------------------------------------------

## 1. Clone the Repository

``` bash
git clone <your-repository-url>
cd walmart-airflow-dbt
```

------------------------------------------------------------------------

## 2. Configure Environment Variables

Create a local `.env` file based on:

``` text
.env.example
```

Do not commit `.env` to GitHub.

Example configuration:

``` env
AIRFLOW_UID=50000

DATABRICKS_HOST=<your-databricks-host>
DATABRICKS_TOKEN=<your-databricks-token>
DATABRICKS_JOB_ID=<your-databricks-job-id>
```

Use your actual local configuration and secret-management approach.

------------------------------------------------------------------------

## 3. Start the Docker Environment

``` bash
docker compose up -d
```

Check running containers:

``` bash
docker compose ps
```

View Airflow logs if required:

``` bash
docker compose logs -f
```

------------------------------------------------------------------------

## 4. Access Airflow

Open the Airflow web interface exposed by your Docker Compose
configuration.

From the Airflow UI:

1.  Locate the project DAG.
2.  Enable the DAG.
3.  Trigger it manually.
4.  Monitor task execution.
5.  Inspect logs for failed tasks.
6.  Verify successful downstream transformations.

------------------------------------------------------------------------

## 5. Run dbt

From the dbt project directory:

``` bash
dbt deps
```

Then:

``` bash
dbt debug
```

Compile the project:

``` bash
dbt compile
```

Run models:

``` bash
dbt run
```

Run tests:

``` bash
dbt test
```

Generate documentation:

``` bash
dbt docs generate
```

Serve documentation locally:

``` bash
dbt docs serve
```

------------------------------------------------------------------------

# Environment Variables

The project may require environment-specific configuration such as:

``` text
DATABRICKS_HOST
DATABRICKS_TOKEN
DATABRICKS_JOB_ID
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_REGION
AIRFLOW_UID
```

Secrets should be supplied through:

-   environment variables
-   Airflow Connections
-   secret managers
-   CI/CD secrets

Do not store credentials directly inside DAGs or committed configuration
files.

------------------------------------------------------------------------

# Example Pipeline Execution

A typical run looks like:

``` text
1. Source data arrives
          ↓
2. CDC / file ingestion
          ↓
3. Databricks processes incoming data
          ↓
4. Incremental layer updated
          ↓
5. Airflow starts downstream workflow
          ↓
6. Source freshness validated
          ↓
7. dbt Silver Technical models execute
          ↓
8. Silver quality tests execute
          ↓
9. Business OBT is created
          ↓
10. Business quality checks execute
          ↓
11. Gold models execute
          ↓
12. Snapshots maintain historical versions
          ↓
13. Fact models are generated
          ↓
14. Analytics-ready data becomes available
```

------------------------------------------------------------------------

# Engineering Concepts Demonstrated

This project demonstrates several real-world data engineering concepts:

### Data Engineering

-   ELT architecture
-   Incremental data processing
-   Change Data Capture
-   Batch/file ingestion
-   Data transformation
-   Data quality
-   Dimensional modeling

### Databricks / Delta

-   Databricks-based processing
-   Delta Lake
-   Incremental processing
-   Reliable analytical storage

### dbt

-   dbt models
-   `ref()`
-   Incremental models
-   Ephemeral models
-   Snapshots
-   SCD Type 2
-   Generic tests
-   Singular/custom tests
-   Source freshness
-   Macros
-   Model documentation
-   Model dependencies

### Airflow

-   DAGs
-   Task dependencies
-   Scheduling
-   Orchestration
-   Retries
-   Logging
-   Pipeline monitoring

### DevOps

-   Docker
-   Environment configuration
-   Git
-   GitHub
-   Reproducible development environment

------------------------------------------------------------------------

# Why This Architecture?

The architecture separates responsibilities between different
technologies.

  Layer               Responsibility
  ------------------- -----------------------------
  Agentic DB          Operational source
  AWS S3              File-based ingestion source
  CDC                 Capture database changes
  Databricks          Data processing
  Incremental Layer   Efficient change processing
  Airflow             Orchestration
  dbt                 Transformation and testing
  Delta Lake          Reliable data storage
  Gold Layer          Analytics-ready models

This separation makes the system easier to:

-   maintain
-   test
-   debug
-   scale
-   monitor
-   extend

------------------------------------------------------------------------

# Future Improvements

Potential improvements include:

-   Add GitHub Actions CI/CD
-   Add automated dbt test execution in CI
-   Add Slack/email failure notifications
-   Add centralized secret management
-   Add pipeline monitoring and alerting
-   Add data observability
-   Add Power BI dashboards
-   Add automated dbt documentation deployment
-   Add performance monitoring
-   Add infrastructure-as-code
-   Add cloud deployment automation
-   Add more advanced CDC handling
-   Add SLA monitoring

------------------------------------------------------------------------

# Project Highlights

The key engineering capabilities demonstrated by this project are:

``` text
        ┌─────────────────────────────┐
        │       Data Sources          │
        └──────────────┬──────────────┘
                       │
                       ▼
        ┌─────────────────────────────┐
        │       CDC + File Ingestion  │
        └──────────────┬──────────────┘
                       │
                       ▼
        ┌─────────────────────────────┐
        │        Databricks           │
        │    Incremental Processing   │
        └──────────────┬──────────────┘
                       │
                       ▼
        ┌─────────────────────────────┐
        │       Apache Airflow        │
        │       Orchestration         │
        └──────────────┬──────────────┘
                       │
                       ▼
        ┌─────────────────────────────┐
        │             dbt             │
        │ Transform + Test + Document │
        └──────────────┬──────────────┘
                       │
                       ▼
        ┌─────────────────────────────┐
        │       Gold Data Models      │
        │   Facts + Dimensions + SCD  │
        └─────────────────────────────┘
```

------------------------------------------------------------------------

# Author

**Aayush Kumar**

Data Engineering \| Python \| SQL \| Apache Airflow \| dbt \| Databricks
\| Delta Lake

------------------------------------------------------------------------

## Disclaimer

This project is created for educational and portfolio purposes.
Walmart-related data used in the project should be treated as
sample/demo data unless explicitly sourced from an authorized public
dataset.
