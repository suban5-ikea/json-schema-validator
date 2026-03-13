# BigQuery Schema Validator

A Python tool to validate JSON payloads from BigQuery invalid records against a JSON schema specification.

## Features

- Connects to BigQuery using OAuth credentials (Application Default Credentials)
- Queries invalid records that haven't been successfully revalidated
- Validates JSON payloads against a provided JSON schema
- Detects additional properties not defined in the schema
- Allows null values for non-required fields
- Outputs detailed validation results including payload and error messages
- Provides error summary grouped by frequency

## Installation

1. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

2. Authenticate with Google Cloud:

   ```bash
   gcloud auth application-default login
   ```

## Usage

```bash
python bq_schema_validator.py <dataset_name> <version> <schema_file> <schema_name>
```

### Arguments

- `dataset_name`: The BigQuery dataset name (without project prefix)
- `version`: Version prefix for table names (e.g., 'v1', 'v2')
- `schema_file`: Path to the schema specification file (YAML or JSON)
- `schema_name`: Name of the schema within the spec file to validate against

### Options

- `--dry-run`: Print the generated SQL query without executing it

### Examples

```bash
# Validate records using a schema from a YAML spec file (v1 tables)
python bq_schema_validator.py topology_bay v1 ../topology-bay.yml whm-topology-addbay-v1

# Validate records using v2 tables
python bq_schema_validator.py my_dataset v2 ../spec.json my-schema-name

# Preview the query without executing
python bq_schema_validator.py topology_bay v1 ../topology-bay.yml whm-topology-addbay-v1 --dry-run
```

## How It Works

1. Constructs table names from the dataset and version:
   - Landing table: `ingka-fms-dataplatform-prod.<dataset_name>.<version>_landing`
   - Invalid table: `ingka-fms-dataplatform-prod.<dataset_name>.<version>_invalid`

2. Executes a query to fetch records from the landing table where:
   - The `message_id` exists in the invalid table
   - The record has NOT been successfully revalidated

3. Validates each JSON payload against the provided schema:
   - Flags additional properties not defined in the schema
   - Allows null values for non-required fields
   - Reports all validation errors (not just the first one)

4. Outputs validation results showing:
   - The payload content
   - Any validation errors with their paths
   - Error summary grouped by frequency

## Schema File Format

The tool supports both YAML and JSON specification files. It looks for schemas in these locations:
- `schemas.<schema_name>`
- `components.schemas.<schema_name>`

Example YAML structure (AsyncAPI format):

```yaml
components:
  schemas:
    whm-topology-addbay-v1:
      type: object
      required:
        - businessUnitType
      properties:
        businessUnitType:
          type: string
```

## Output Format

```text
================================================================================
VALIDATION RESULTS
================================================================================

--- Record 1 ---
Payload:
{
  "field1": "value1",
  ...
}

Errors (2):
  - [field1] 'value1' is not of type 'integer'
  - [field2 -> nested] 'required' is a required property

================================================================================
SUMMARY: 5 valid, 3 invalid out of 8 records
================================================================================

================================================================================
ERROR SUMMARY (grouped by frequency)
================================================================================

1. [5 payload(s) affected]
   [body -> extraField] Additional properties are not allowed

2. [3 payload(s) affected]
   [header -> type] 'INVALID' is not one of ['CREATE', 'UPDATE', 'DELETE']

--------------------------------------------------------------------------------
Total unique errors: 2
Total error occurrences: 8
================================================================================
```

## Requirements

- Python 3.9+
- Google Cloud SDK (for authentication)
- Access to the BigQuery dataset
