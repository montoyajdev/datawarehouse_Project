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

# Data Catalog for Gold Layer

## Overview
The **Gold Layer** represents the business-level data in our data warehouse, meticulously structured to support analytical and reporting use cases. It is designed using a star schema model, comprising dimension tables (contextual data) and fact tables (business metrics) to facilitate efficient querying and insights generation.

---

## Dimension and Fact Tables

### 1. `gold.dim_customers`
- **Purpose**: Stores detailed customer information enriched with demographic and geographic data for business analysis.

| Column Name        | Data Type     | Description                                                                 |
|--------------------|---------------|-----------------------------------------------------------------------------|
| customer_key       | INT           | Surrogate key uniquely identifying each customer record in the dimension table. |
| customer_id        | INT           | Unique numerical identifier assigned to each customer.                     |
| customer_number    | VARCHAR(50)   | Alphanumeric identifier representing the customer, used for tracking and referencing. |
| first_name         | VARCHAR(50)   | The customer's first name, as recorded in the system.                      |
| last_name          | VARCHAR(50)   | The customer's last name or family name.                                   |
| country            | VARCHAR(50)   | The country of residence for the customer (e.g., 'Australia').             |
| marital_status     | VARCHAR(50)   | The marital status of the customer (e.g., 'Married', 'Single').            |
| gender             | VARCHAR(50)   | The gender of the customer (e.g., 'Male', 'Female', 'n/a').                |
| birthdate          | DATE          | The date of birth of the customer, formatted as YYYY-MM-DD (e.g., 1971-10-06). |
| create_date        | DATE          | The date and time when the customer record was created in the system.      |

---

### 2. `gold.dim_products`
- **Purpose**: Provides detailed information about products and their attributes for inventory and sales analysis.

| Column Name          | Data Type     | Description                                                                 |
|----------------------|---------------|-----------------------------------------------------------------------------|
| product_key          | INT           | Surrogate key uniquely identifying each product record in the dimension table. |
| product_id           | INT           | A unique identifier assigned to the product for internal tracking and referencing. |
| product_number       | VARCHAR(50)   | A structured alphanumeric code representing the product, often used for categorization or inventory. |
| product_name         | VARCHAR(50)   | Descriptive name of the product, including key details such as type, color, and size. |
| category_id          | VARCHAR(50)   | A unique identifier for the product's category, linking to its high-level classification. |
| category             | VARCHAR(50)   | The broader classification of the product (e.g., Bikes, Components) to group related items. |
| subcategory          | VARCHAR(50)   | A more detailed classification of the product within the category, such as product type. |
| maintenance_required | VARCHAR(50)   | Indicates whether the product requires maintenance (e.g., 'Yes', 'No').    |
| cost                 | INT           | The cost or base price of the product, measured in monetary units.         |
| product_line         | VARCHAR(50)   | The specific product line or series to which the product belongs (e.g., Road, Mountain). |
| start_date           | DATE          | The date when the product became available for sale or use.                |

---

### 3. `gold.fact_sales`
- **Purpose**: Stores transactional sales data for analytical purposes, enabling revenue and performance reporting.

| Column Name        | Data Type     | Description                                                                 |
|--------------------|---------------|-----------------------------------------------------------------------------|
| order_number       | VARCHAR(50)   | A unique alphanumeric identifier for each sales order (e.g., 'SO54496').   |
| product_key        | INT           | Surrogate key linking the order to the product dimension table.            |
| customer_key       | INT           | Surrogate key linking the order to the customer dimension table.           |
| order_date         | DATE          | The date when the order was placed.                                        |
| shipping_date      | DATE          | The date when the order was shipped to the customer.                       |
| due_date           | DATE          | The date when the order payment was due.                                   |
| sales_amount       | INT           | The total monetary value of the sale for the line item, in whole currency units (e.g., 25). |
| quantity           | INT           | The number of units of the product ordered for the line item (e.g., 1).    |
| price              | INT           | The price per unit of the product for the line item, in whole currency units (e.g., 25). |

---

## Notes
- The Gold Layer is optimized for business intelligence tools and reporting frameworks, ensuring quick access to aggregated data.
- Surrogate keys (`customer_key`, `product_key`) are used to maintain referential integrity across dimension and fact tables.
