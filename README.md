# Data Warehouse Project

Welcome to the **Data Warehouse Project**! This project is designed to manage and transform data through a multi-layered architecture, ensuring data is processed efficiently from raw ingestion to business-ready insights.

## Architecture Overview

- **Bronze Layer**: 
  - Stores raw data exactly as it is received from source systems.
  - Data is ingested directly from CSV files into a PostgreSQL database.
  
- **Silver Layer**: 
  - Focuses on data cleansing, standardization, and normalization.
  - Prepares the data for downstream analysis by improving quality and consistency.
  
- **Gold Layer**: 
  - Contains business-ready data modeled into a star schema.
  - Optimized for reporting, analytics, and business intelligence purposes.

## Purpose
This layered approach ensures a clear separation of concerns, making data management scalable, maintainable, and suitable for advanced analytics.
