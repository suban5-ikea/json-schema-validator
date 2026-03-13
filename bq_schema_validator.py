#!/usr/bin/env python3
"""
BigQuery Schema Validator

This script validates JSON payloads from BigQuery invalid records against a JSON schema.

Usage:
    python bq_schema_validator.py <dataset_name> <version> <schema_file> <schema_name>

Example:
    python bq_schema_validator.py my_dataset v1 spec.yaml whm-topology-addbay-v1
"""

import argparse
import copy
import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

from google.cloud import bigquery
import google.auth
import jsonschema
from jsonschema import Draft7Validator, ValidationError
import yaml


def json_serial(obj: Any) -> str:
    """JSON serializer for objects not serializable by default json code."""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def deep_copy_with_json(obj: Any) -> Any:
    """Deep copy an object, converting non-JSON-serializable types to strings."""
    return json.loads(json.dumps(obj, default=json_serial))


def get_bigquery_client() -> bigquery.Client:
    """
    Create a BigQuery client using OAuth credentials.
    Uses Application Default Credentials (ADC).
    
    Returns:
        bigquery.Client: Authenticated BigQuery client
    """
    # Use default credentials (OAuth or service account)
    credentials, project = google.auth.default(
        scopes=["https://www.googleapis.com/auth/bigquery.readonly"]
    )
    return bigquery.Client(credentials=credentials, project=project)


def build_table_names(project_name: str,dataset_name: str, version: str) -> tuple[str, str]:
    """
    Build landing and invalid table names from dataset name and version.
    
    Args:
        project_name: The BigQuery project name
        dataset_name: The BigQuery dataset name
        version: The version prefix for table names (e.g., 'v1', 'v2')
        
    Returns:
        Tuple of (landing_table, invalid_table) fully qualified names
    """
    landing_table = f"{project_name}.{dataset_name}.{version}_landing"
    invalid_table = f"{project}.{dataset_name}.{version}_invalid"
    return landing_table, invalid_table


def build_query(landing_table: str, invalid_table: str) -> str:
    """
    Build the SQL query to fetch invalid records that haven't been revalidated.
    
    Args:
        landing_table: Fully qualified landing table name
        invalid_table: Fully qualified invalid table name
        
    Returns:
        SQL query string
    """
    query = f"""
    SELECT data 
    FROM `{landing_table}` 
    WHERE message_id IN (
        SELECT source_message_id 
        FROM `{invalid_table}` 
        WHERE 1=1
        AND source_message_id NOT IN (
            SELECT source_message_id 
            FROM `{invalid_table}` 
            WHERE revalidate_successful
        )
    )
    """
    return query


def load_spec_file(spec_file: str) -> dict:
    """
    Load specification file (YAML or JSON).
    
    Args:
        spec_file: Path to the specification file
        
    Returns:
        Specification content as dictionary
    """
    file_path = Path(spec_file)
    with open(spec_file, 'r') as f:
        if file_path.suffix.lower() in ['.yaml', '.yml']:
            return yaml.safe_load(f)
        else:
            return json.load(f)


def get_schema_from_spec(spec: dict, schema_name: str) -> dict:
    """
    Extract a specific schema from the specification file and resolve all $ref references.
    
    Looks for the schema in common locations:
    - spec['schemas'][schema_name]
    - spec['components']['schemas'][schema_name]
    
    Args:
        spec: The loaded specification dictionary
        schema_name: Name of the schema to extract
        
    Returns:
        The schema dictionary with all references resolved or embedded
        
    Raises:
        KeyError: If schema is not found
    """
    # Find the schemas container
    schemas_container = None
    if 'schemas' in spec:
        schemas_container = spec['schemas']
    elif 'components' in spec and 'schemas' in spec['components']:
        schemas_container = spec['components']['schemas']
    
    if schemas_container is None or schema_name not in schemas_container:
        # List available schemas for error message
        available = []
        if 'schemas' in spec:
            available.extend(spec['schemas'].keys())
        if 'components' in spec and 'schemas' in spec['components']:
            available.extend(spec['components']['schemas'].keys())
        raise KeyError(
            f"Schema '{schema_name}' not found. Available schemas: {', '.join(available) if available else 'none'}"
        )
    
    # Get the target schema
    target_schema = schemas_container[schema_name]
    
    # Deep copy and add additionalProperties: false to all object schemas
    # to catch unknown/extra properties not defined in the spec
    schemas_with_strict_props = _add_additional_properties_false(
        deep_copy_with_json(schemas_container)
    )
    
    target_schema_strict = _add_additional_properties_false(
        deep_copy_with_json(target_schema)
    )
    
    # Build a complete JSON Schema document with all schemas as definitions
    # This allows $ref resolution to work properly
    complete_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "components": {
            "schemas": schemas_with_strict_props
        },
        **target_schema_strict
    }
    
    return complete_schema


def _add_additional_properties_false(schema: Any, required_fields: set = None) -> Any:
    """
    Recursively process schema to:
    1. Add 'additionalProperties': false to all object schemas with 'properties'
    2. Allow null for non-required properties by changing type to [type, 'null']
    
    This ensures that:
    - Extra properties not in the spec are flagged as errors
    - Optional fields can have null values
    
    Args:
        schema: The schema or sub-schema to modify
        required_fields: Set of required field names (passed down for property processing)
        
    Returns:
        Modified schema
    """
    if isinstance(schema, dict):
        result = {}
        
        # Get required fields for this object level
        current_required = set(schema.get('required', []))
        
        for key, value in schema.items():
            if key == 'properties' and isinstance(value, dict):
                # Process properties, passing required info
                result[key] = {}
                for prop_name, prop_schema in value.items():
                    processed = _add_additional_properties_false(prop_schema, current_required)
                    # If property is not required and has a simple type, allow null
                    if prop_name not in current_required and isinstance(processed, dict):
                        if 'type' in processed and processed['type'] != 'null':
                            current_type = processed['type']
                            # Don't modify if already allows null
                            if isinstance(current_type, list):
                                if 'null' not in current_type:
                                    processed['type'] = current_type + ['null']
                            else:
                                processed['type'] = [current_type, 'null']
                    result[key][prop_name] = processed
            else:
                result[key] = _add_additional_properties_false(value, required_fields)
        
        # Add additionalProperties: false to objects with properties defined
        if result.get('type') == 'object' and 'properties' in result:
            if 'additionalProperties' not in result:
                result['additionalProperties'] = False
        
        return result
    elif isinstance(schema, list):
        return [_add_additional_properties_false(item, required_fields) for item in schema]
    else:
        return schema


def validate_payload(payload: Any, schema: dict) -> list[str]:
    """
    Validate a JSON payload against a schema and return ALL validation errors.
    
    Args:
        payload: The JSON payload to validate
        schema: The JSON schema to validate against
        
    Returns:
        List of all error messages (empty if valid)
    """
    validator = Draft7Validator(schema)
    errors = []
    
    # iter_errors returns all validation errors, not just the first one
    for error in sorted(validator.iter_errors(payload), key=lambda e: list(e.absolute_path)):
        error_path = " -> ".join(str(p) for p in error.absolute_path) if error.absolute_path else "root"
        # Include the schema path for more context
        schema_path = " -> ".join(str(p) for p in error.absolute_schema_path) if error.absolute_schema_path else ""
        
        error_msg = f"[{error_path}] {error.message}"
        if error.validator == 'required':
            error_msg = f"[{error_path}] {error.message}"
        elif error.validator == 'type':
            error_msg = f"[{error_path}] {error.message} (got {type(error.instance).__name__})"
        elif error.validator == 'enum':
            error_msg = f"[{error_path}] {error.message}"
        elif error.validator == 'additionalProperties':
            error_msg = f"[{error_path}] {error.message}"
        
        errors.append(error_msg)
    
    return errors


def execute_query(client: bigquery.Client, query: str) -> list[dict]:
    """
    Execute BigQuery query and return results.
    
    Args:
        client: BigQuery client
        query: SQL query to execute
        
    Returns:
        List of row dictionaries
    """
    print(f"Executing query...")
    query_job = client.query(query)
    results = query_job.result()
    
    rows = []
    for row in results:
        rows.append(dict(row))
    
    print(f"Retrieved {len(rows)} records")
    return rows


def print_validation_results(results: list[dict]) -> None:
    """
    Print validation results in a formatted way.
    
    Args:
        results: List of validation result dictionaries
    """
    from collections import Counter
    
    print("\n" + "=" * 80)
    print("VALIDATION RESULTS")
    print("=" * 80)
    
    valid_count = 0
    invalid_count = 0
    error_counter = Counter()  # Track error frequency
    
    for i, result in enumerate(results, 1):
        print(f"\n--- Record {i} ---")
        
        # Pretty print the payload (truncated if too long)
        payload_str = json.dumps(result['payload'], indent=2, default=str)
        if len(payload_str) > 500:
            payload_str = payload_str[:500] + "\n... (truncated)"
        print(f"Payload:\n{payload_str}")
        
        if result['errors']:
            invalid_count += 1
            print(f"\nErrors ({len(result['errors'])}):")
            for error in result['errors']:
                print(f"  - {error}")
                error_counter[error] += 1
        else:
            valid_count += 1
            print("\nStatus: VALID")
    
    print("\n" + "=" * 80)
    print(f"SUMMARY: {valid_count} valid, {invalid_count} invalid out of {len(results)} records")
    print("=" * 80)
    
    # Print error summary grouped by frequency
    if error_counter:
        print("\n" + "=" * 80)
        print("ERROR SUMMARY (grouped by frequency)")
        print("=" * 80)
        
        # Sort errors by frequency (most common first)
        sorted_errors = error_counter.most_common()
        
        for idx, (error_msg, count) in enumerate(sorted_errors, 1):
            print(f"\n{idx}. [{count} payload(s) affected]")
            print(f"   {error_msg}")
        
        print("\n" + "-" * 80)
        print(f"Total unique errors: {len(error_counter)}")
        print(f"Total error occurrences: {sum(error_counter.values())}")
        print("=" * 80)


def main():
    parser = argparse.ArgumentParser(
        description="Validate BigQuery invalid records against a JSON schema"
    )
    parser.add_argument(
        "dataset_name",
        help="BigQuery dataset name (e.g., 'my_dataset')"
    )
    parser.add_argument(
        "version",
        help="Version prefix for table names (e.g., 'v1', 'v2'). Tables will be named <version>_landing and <version>_invalid"
    )
    parser.add_argument(
        "schema_file",
        help="Path to the schema specification file (YAML or JSON)"
    )
    parser.add_argument(
        "schema_name",
        help="Name of the schema within the spec file (e.g., 'whm-topology-addbay-v1')"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the query without executing it"
    )
    
    args = parser.parse_args()
    
    # Build table names
    landing_table, invalid_table = build_table_names(args.dataset_name, args.version)
    print(f"Landing table: {landing_table}")
    print(f"Invalid table: {invalid_table}")
    
    # Build query
    query = build_query(landing_table, invalid_table)
    
    if args.dry_run:
        print(f"\nGenerated SQL Query:\n{query}")
        return
    
    # Load schema from spec file
    try:
        spec = load_spec_file(args.schema_file)
        schema = get_schema_from_spec(spec, args.schema_name)
        print(f"Loaded schema '{args.schema_name}' from: {args.schema_file}")
    except FileNotFoundError:
        print(f"Error: Schema file not found: {args.schema_file}")
        sys.exit(1)
    except (json.JSONDecodeError, yaml.YAMLError) as e:
        print(f"Error: Invalid format in schema file: {e}")
        sys.exit(1)
    except KeyError as e:
        print(f"Error: {e}")
        sys.exit(1)
    
    # Connect to BigQuery
    try:
        client = get_bigquery_client()
        print("Connected to BigQuery using OAuth credentials")
    except Exception as e:
        print(f"Error connecting to BigQuery: {e}")
        sys.exit(1)
    
    # Execute query
    try:
        rows = execute_query(client, query)
    except Exception as e:
        print(f"Error executing query: {e}")
        sys.exit(1)
    
    if not rows:
        print("No invalid records found to validate.")
        return
    
    # Validate each payload
    validation_results = []
    for row in rows:
        data = row.get('data')
        
        # Convert data to proper JSON/dict format
        # BigQuery may return various types: string, dict, or struct-like objects
        if data is None:
            validation_results.append({
                'payload': None,
                'errors': ["Payload is null/empty"]
            })
            continue
        
        # If it's a string, parse it as JSON
        if isinstance(data, str):
            try:
                payload = json.loads(data)
            except json.JSONDecodeError as e:
                validation_results.append({
                    'payload': data,
                    'errors': [f"Invalid JSON: {e}"]
                })
                continue
        else:
            # Convert to JSON and back to ensure it's a plain dict
            # This handles BigQuery Row objects and other complex types
            try:
                payload = json.loads(json.dumps(data, default=str))
            except (TypeError, json.JSONDecodeError) as e:
                validation_results.append({
                    'payload': str(data),
                    'errors': [f"Failed to convert payload to JSON: {e}"]
                })
                continue
        
        errors = validate_payload(payload, schema)
        validation_results.append({
            'payload': payload,
            'errors': errors
        })
    
    # Print results
    print_validation_results(validation_results)


if __name__ == "__main__":
    main()
