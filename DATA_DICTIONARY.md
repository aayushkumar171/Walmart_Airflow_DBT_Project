# Data Dictionary

This document provides a high-level description of the main datasets in the Walmart data engineering pipeline.

## Source / Bronze

| Dataset | Description |
|---|---|
| `orders` | Order-level source data |
| `order_items` | Individual items associated with orders |
| `customers` | Customer master/source data |
| `products` | Product master/source data |
| `stores` | Store information |
| `employees` | Employee information |

## Silver Technical

| Model | Description |
|---|---|
| `customers_t` | Transformed customer records |
| `employees_t` | Transformed employee records |
| `orders_t` | Transformed order records |
| `order_items_t` | Transformed order-item records |
| `products_t` | Transformed product records |
| `stores_t` | Transformed store records |

## Silver Business

| Model | Description |
|---|---|
| `obt_b` | Business-level one-big-table combining the main Silver entities |

## Gold Intermediate

| Model | Description |
|---|---|
| `eph_customers` | Customer dimension-oriented intermediate model |
| `eph_employees` | Employee dimension-oriented intermediate model |
| `eph_orders` | Order dimension-oriented intermediate model |
| `eph_products` | Product dimension-oriented intermediate model |
| `eph_stores` | Store dimension-oriented intermediate model |

These models are configured as dbt ephemeral models.

## Gold Snapshots

| Snapshot | Purpose |
|---|---|
| `dim_customers` | Historical customer records |
| `dim_employees` | Historical employee records |
| `dim_orders` | Historical order records |
| `dim_products` | Historical product records |
| `dim_stores` | Historical store records |

The snapshots use dbt's timestamp strategy to retain historical versions.

## Gold Fact

| Model | Description |
|---|---|
| `fact_orders` | Analytical order/order-item fact model containing order, product, store, employee and customer identifiers plus measures such as quantity, unit price and line amount |

## Data Quality

The project includes checks for:

- Not-null identifiers
- Unique identifiers
- Product price condition
- OBT identifier completeness
- Source freshness

For exact test definitions, refer to the dbt model YAML files and `tests/test_obt.sql`.
