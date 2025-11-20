# Databricks

This repository contains Databricks utilities and data generation scripts for working with Delta tables.

## Repository Contents

- `001_script_generate_data.py` - Script to generate sample data for Databricks workshops
- `utilities_databricks.py` - Utility functions for working with Databricks and Parquet files

## Adding User Access to This Repository

### GitHub Repository Access

To add a user as a collaborator to this GitHub repository:

1. **Navigate to the repository** on GitHub: https://github.com/FranciscoBelo/Databricks

2. **Go to Settings**:
   - Click on the "Settings" tab in the repository menu

3. **Access Collaborators**:
   - Click on "Collaborators and teams" in the left sidebar
   - You may need to confirm your password

4. **Add a Collaborator**:
   - Click the "Add people" button
   - Enter the GitHub username or email of the person you want to add
   - Select their access level:
     - **Read**: Can view and clone the repository
     - **Write**: Can read and push to the repository
     - **Admin**: Full access including settings
   - Click "Add [username] to this repository"

5. **Confirm**:
   - The user will receive an invitation email
   - They need to accept the invitation to gain access

### For Organization Repositories

If this repository is part of an organization:

1. Go to the organization's page
2. Click "People" or "Teams"
3. Add the user to the organization first
4. Then add them to specific repositories or teams

### Databricks Workspace Access

If you need to grant access to the Databricks workspace where these scripts run:

1. **Log in to Databricks** workspace as an admin

2. **Navigate to Admin Settings**:
   - Click on your username in the top right
   - Select "Admin Settings" or "Settings"

3. **Add Users**:
   - Go to the "Users" tab
   - Click "Add User"
   - Enter the user's email address
   - Assign appropriate permissions

4. **Grant Catalog/Schema Permissions** (for Unity Catalog):
   ```sql
   -- Grant usage on catalog
   GRANT USAGE ON CATALOG databrickstheloop TO `user@example.com`;
   
   -- Grant usage on schema
   GRANT USAGE ON SCHEMA databrickstheloop.databricksfundamentals TO `user@example.com`;
   
   -- Grant create permissions if needed
   GRANT CREATE ON SCHEMA databrickstheloop.databricksfundamentals TO `user@example.com`;
   
   -- Grant select on tables
   GRANT SELECT ON SCHEMA databrickstheloop.databricksfundamentals TO `user@example.com`;
   ```

## Usage

### Data Generation Script

The `001_script_generate_data.py` script generates sample data for a Databricks data warehouse including:

- Date dimension (`dim_date`)
- Product dimension (`dim_product`)
- Customer dimension (`dim_customer`)
- Store dimension (`dim_store`)
- Sales fact table (`fact_sales`)

**Configuration**: Update the catalog and schema names at the top of the script to match your Databricks environment.

### Utilities

The `utilities_databricks.py` module provides helper functions for reading Parquet files with metadata filtering.

## Requirements

- Apache Spark with Delta Lake support
- Databricks Runtime (for `databricks.sdk.runtime`)
- Python 3.x

## Contributing

To contribute to this repository, please ensure you have the appropriate access permissions as described above.