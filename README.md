# E-Commerce PySpark to Snowflake ETL

An end-to-end data engineering pipeline that generates synthetic e-commerce data and uses PySpark to transform and load it into a Snowflake data warehouse.

## Project Structure

- `generate_data.py`: Creates synthetic Customers, Products, Orders, and Reviews CSV files.
- `etl.py`: PySpark job that cleans, joins, and aggregates data, then loads it into Snowflake.
- `snowflake_setup.sql`: SQL commands to initialize the Snowflake environment (database, warehouse, schema).
- `.env`: Configuration file for Snowflake credentials.

## Setup

1. **Snowflake Setup**: Run the commands in `snowflake_setup.sql` in your Snowflake worksheet.
2. **Environment Variables**: Configure your `.env` file with your Snowflake account details.
3. **Environment**: Recommended Python 3.10+ and Java 17 for PySpark compatibility.

## Usage

1. **Generate Data**:
   ```bash
   python generate_data.py
   ```

2. **Run ETL Pipeline**:
   ```bash
   python etl.py
   ```

## Key Metrics Computed
- Daily sales revenue.
- Daily total profit.
- Order volume trends.
- Customer geography segments.
