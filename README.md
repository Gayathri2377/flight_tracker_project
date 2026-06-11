# ✈ Real-Time Flight Tracking & Analytics Platform

## Overview

This project is a cloud-native flight tracking and analytics platform built on Databricks using the Medallion Architecture (Bronze → Silver → Gold).

Aircraft telemetry is collected from the ADSB.lol public API, ingested into Unity Catalog Volumes, processed through Delta Lake tables, and transformed into analytics-ready datasets for Power BI reporting.

### Technology Stack

* Databricks
* Unity Catalog
* Delta Lake
* Python (PySpark)
* ADSB.lol API
* Databricks Workflows
* Power BI

---

# Architecture

```text
ADSB.lol API
      │
      ▼
Bronze Layer
(Unity Catalog Volume → Delta Table)
flight_raw
      │
      ▼
Silver Layer
├── flight_clean
├── flight_current
├── flight_history (SCD Type 2)
└── flight_cdc (Change Data Capture)
      │
      ▼
Gold Layer
├── flight_metrics
├── route_summary
└── airspace_hotspots
      │
      ▼
Power BI Dashboard
```

---

# Data Pipeline

## Bronze Layer

Purpose:

* Store raw ADSB API responses
* Preserve source data for auditing and replay
* Maintain ingestion timestamps

Source:

* ADSB.lol API

Output Table:

* main.flight_tracker.flight_raw

---

## Silver Layer

Purpose:

* Clean and standardize aircraft records
* Apply validation rules
* Create current-state and historical views
* Implement CDC and SCD Type 2 tracking

Tables:

### flight_clean

Validated aircraft records.

### flight_current

Latest known position per aircraft.

### flight_history

Historical aircraft state tracking using SCD Type 2.

### flight_cdc

Change Data Capture events for aircraft updates.

---

## Gold Layer

Purpose:

* Business-ready analytics datasets
* Aggregated reporting tables
* Power BI consumption layer

Tables:

### flight_metrics

Operational flight KPIs and metrics.

### route_summary

Aggregated route-level analytics.

### airspace_hotspots

High-density airspace activity analysis.

---

# Unity Catalog Objects

Catalog:
main

Schema:
flight_tracker

Volume:
adsb_raw

---

# Project Structure

```text
flight_tracker/
│
├── notebooks/
│   ├── 00_setup
│   ├── 01_bronze_ingestion
│   ├── 02_silver_processing
│   ├── 03_gold_analytics
│   └── 04_validation
│
├── sql/
│   ├── create_objects.sql
│   ├── cleanup_tables.sql
│   └── validation_queries.sql
│
├── powerbi/
│   └── FlightTracker.pbix
│
├── docs/
│   ├── Architecture.png
│   └── Project_Documentation.pdf
│
└── README.md
```

---

# Quick Start

Estimated Setup Time: 30–60 Minutes

## Step 1 – Create Databricks Workspace

Create a Databricks workspace with Unity Catalog enabled.

## Step 2 – Import Notebooks

Import all project notebooks into Databricks Workspace.

## Step 3 – Create Catalog Objects

Run:

00_setup

This creates:

* Catalog
* Schema
* Volume
* Delta Tables

## Step 4 – Run Bronze Layer

Execute:

01_bronze_ingestion

Verify:

```sql
SELECT COUNT(*) FROM main.flight_tracker.flight_raw;
```

## Step 5 – Run Silver Layer

Execute:

02_silver_processing

Verify:

```sql
SELECT COUNT(*) FROM main.flight_tracker.flight_clean;
```

## Step 6 – Run Gold Layer

Execute:

03_gold_analytics

Verify:

```sql
SELECT COUNT(*) FROM main.flight_tracker.flight_metrics;
```

## Step 7 – Connect Power BI

Connect Power BI to Databricks using:

* Databricks Connector
* Unity Catalog Tables

Recommended tables:

* flight_metrics
* route_summary
* airspace_hotspots

---

# Operational Features

* Real-Time Flight Monitoring
* Change Data Capture (CDC)
* Slowly Changing Dimensions (SCD Type 2)
* Delta Lake Storage
* Unity Catalog Governance
* Historical Flight Tracking
* Power BI Analytics
* Automated Databricks Workflows

---

# Data Quality Checks

```sql
SELECT MIN(fetched_at), MAX(fetched_at)
FROM main.flight_tracker.flight_raw;
```

```sql
SELECT COUNT(*)
FROM main.flight_tracker.flight_clean
WHERE hex IS NULL;
```

```sql
SELECT COUNT(*)
FROM main.flight_tracker.flight_current;
```

---

# Reset Pipeline

To rebuild the project from scratch:

```sql
TRUNCATE TABLE main.flight_tracker.flight_raw;
TRUNCATE TABLE main.flight_tracker.flight_clean;
TRUNCATE TABLE main.flight_tracker.flight_current;
TRUNCATE TABLE main.flight_tracker.flight_history;
TRUNCATE TABLE main.flight_tracker.flight_cdc;
TRUNCATE TABLE main.flight_tracker.flight_metrics;
TRUNCATE TABLE main.flight_tracker.route_summary;
TRUNCATE TABLE main.flight_tracker.airspace_hotspots;
```

Re-run Bronze → Silver → Gold.
