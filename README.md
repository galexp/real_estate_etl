real_estate_etl
Real Estate ETL & Analytics Pipeline

Project Overview

This project implements an end-to-end ETL (Extract, Transform, Load) pipeline for real estate data, integrating property listings into a PostgreSQL database and enabling real-time analytics with Tableau.

The system automates data collection, standardizes and cleans records, and provides insights into property performance, pricing trends, and geospatial distributions — ideal for decision-making in a data-driven real estate environment.

Features

Automated Data Extraction

Connects to external APIs (e.g., RentCast) to pull property listings.

Caches API responses locally to reduce redundant requests and cost.

Incremental loading to avoid re-fetching existing properties.

Data Cleaning & Transformation

Renames and standardizes columns for consistency.

Handles missing values, duplicates, and type mismatches.

Enforces a consistent schema for analytics.

Centralized Data Storage

Stores cleaned property data in PostgreSQL.

Designed for scalable querying and reporting.

Includes timestamped incremental loads for tracking updates.

Analytics & Visualization

Connects Tableau Desktop (Mac) directly to PostgreSQL.

Builds interactive dashboards for property performance:

KPI cards (Total Listings, Average Price, Days on Market)

Charts by property type, city, and price trends

Interactive maps with geographic distributions

Version Control & Reproducibility

Python scripts are modular (extract, transform, load, quality).

GitHub repository for version tracking and collaboration.

Installation & Setup

Clone Repository git clone https://github.com/galexp/real_estate_etl.git cd real_estate_etl

Set Up Python Environment python3 -m venv venv source venv/bin/activate pip install -r requirements.txt

Configure PostgreSQL

Install PostgreSQL (local or cloud)

Create database, e.g., real_estate_db

Update db/postgres_client.py with: host="localhost" port=5432 database="real_estate_db" user="your_user" password="your_password"

Run ETL python main.py
