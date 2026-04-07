# Retail Sales Analytics Pipeline — Azure Databricks

End-to-end data pipeline using Medallion architecture (Bronze/Silver/Gold), Delta Lake, and PySpark on Azure Databricks.

## Tech Stack
- **Platform:** Azure Databricks
- **Format:** Delta Lake
- **Language:** PySpark (Python)
- **Data:** Online Retail II dataset — 541,910 transactions

## Pipeline
| Layer | Description |
|---|---|
| Bronze | Raw CSV ingested into Delta format |
| Silver | Cleaned, typed, enriched with LineRevenue column |
| Gold | KPI aggregations: monthly revenue, top products, revenue by country |

## Features Demonstrated
- Medallion architecture
- Delta Lake ACID transactions
- Delta time travel (version rollback)
- Databricks Dashboard visualization

## Dataset
[Online Retail II — UCI ML Repository](https://archive.ics.uci.edu/dataset/502/online+retail+ii)
