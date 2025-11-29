#!/usr/bin/env python3
"""Validate source specification YAML files.

This script validates all source specs in the source_specs directory,
checking for required fields, valid types, consistent configuration,
and XPath syntax for XML sources.

Usage:
    python scripts/validate_specs.py              # Validate all specs
    python scripts/validate_specs.py --source murex_trades  # Validate one spec
"""

import argparse
import sys
from pathlib import Path

import yaml


# Valid BigQuery types
VALID_TYPES = {
    "STRING", "INT64", "FLOAT64", "NUMERIC", "BOOL",
    "TIMESTAMP", "DATE", "TIME", "DATETIME", "BYTES", "JSON"
}

# Required top-level fields
REQUIRED_FIELDS = {"name", "source", "schema"}

# Required source fields
REQUIRED_SOURCE_FIELDS = {"path_pattern", "format"}

# Valid formats
VALID_FORMATS = {"csv", "json", "jsonl", "xml", "parquet"}


def validate_xpath(xpath: str, namespaces: dict) -> tuple[bool, str | None]:
    """Validate XPath expression syntax.
    
    Args:
        xpath: XPath expression to validate
        namespaces: Namespace prefix to URI mapping
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    try:
        from lxml import etree
        
        # Compile the XPath to check syntax
        etree.XPath(xpath, namespaces=namespaces)
        return True, None
        
    except etree.XPathSyntaxError as e:
        return False, f"Invalid XPath syntax: {e}"
    except etree.XPathEvalError as e:
        return False, f"XPath evaluation error: {e}"
    except ImportError:
        # lxml not installed - skip XPath validation with a warning
        return True, None
    except Exception as e:
        return False, f"XPath validation error: {e}"


def validate_spec(spec_path: Path) -> list[str]:
    """Validate a single source specification file.
    
    Args:
        spec_path: Path to the YAML file
        
    Returns:
        List of error messages (empty if valid)
    """
    errors = []
    warnings = []
    
    try:
        with open(spec_path) as f:
            spec = yaml.safe_load(f)
    except yaml.YAMLError as e:
        return [f"YAML parse error: {e}"]
    except Exception as e:
        return [f"Failed to read file: {e}"]
    
    if spec is None:
        return ["File is empty"]
    
    # Check required top-level fields
    for field in REQUIRED_FIELDS:
        if field not in spec:
            errors.append(f"Missing required field: {field}")
    
    if errors:
        return errors  # Can't validate further without required fields
    
    # Validate source section
    source = spec.get("source", {})
    for field in REQUIRED_SOURCE_FIELDS:
        if field not in source:
            errors.append(f"Missing required source field: {field}")
    
    format_type = source.get("format", "").lower()
    if format_type and format_type not in VALID_FORMATS:
        errors.append(f"Invalid format '{format_type}'. Valid: {VALID_FORMATS}")
    
    # Validate schema
    schema = spec.get("schema", [])
    if not schema:
        errors.append("Schema is empty")
    
    # Get namespaces for XPath validation
    xml_config = spec.get("xml_config", {})
    namespaces = xml_config.get("namespaces", {})
    
    field_names = set()
    for i, field in enumerate(schema):
        field_name = field.get("name")
        if not field_name:
            errors.append(f"Schema field {i} missing 'name'")
            continue
        
        if field_name in field_names:
            errors.append(f"Duplicate field name: {field_name}")
        field_names.add(field_name)
        
        field_type = field.get("type", "").upper()
        if not field_type:
            errors.append(f"Field '{field_name}' missing 'type'")
        elif field_type not in VALID_TYPES:
            errors.append(f"Field '{field_name}' has invalid type '{field_type}'. Valid: {VALID_TYPES}")
        
        # Validate constraints consistency
        if "min_value" in field or "max_value" in field:
            if field_type not in {"INT64", "FLOAT64", "NUMERIC"}:
                errors.append(f"Field '{field_name}' has min/max_value but type is {field_type}")
        
        if "allowed_values" in field:
            allowed = field["allowed_values"]
            if not isinstance(allowed, list):
                errors.append(f"Field '{field_name}' allowed_values must be a list")
        
        # Validate XPath for XML sources
        if format_type == "xml":
            xpath = field.get("xpath")
            if not xpath:
                errors.append(f"XML field '{field_name}' missing 'xpath'")
            else:
                is_valid, xpath_error = validate_xpath(xpath, namespaces)
                if not is_valid:
                    errors.append(f"Field '{field_name}' has invalid xpath '{xpath}': {xpath_error}")
    
    # Validate XML-specific config
    if format_type == "xml":
        if "row_element" not in source:
            errors.append("XML format requires 'row_element' in source config")
    
    # Validate control file config if present
    control_file = spec.get("control_file", {})
    if control_file:
        control_type = control_file.get("type")
        valid_control_types = {"sidecar_xml", "sidecar_csv", "trailer"}
        if control_type and control_type not in valid_control_types:
            errors.append(f"Invalid control_file type '{control_type}'. Valid: {valid_control_types}")
        
        # Validate control file has required fields based on type
        if control_type == "sidecar_xml":
            if "xpath_row_count" not in control_file and "pattern" not in control_file:
                warnings.append("sidecar_xml control file should have 'xpath_row_count' or 'pattern'")
        elif control_type == "sidecar_csv":
            if "row_count_field" not in control_file:
                warnings.append("sidecar_csv control file should have 'row_count_field'")
    
    # Print warnings but don't fail
    for warning in warnings:
        print(f"   ⚠ {warning}")
    
    return errors


def main():
    parser = argparse.ArgumentParser(description="Validate source specification YAML files")
    parser.add_argument(
        "--source",
        help="Validate only this source (name without .yaml extension)"
    )
    parser.add_argument(
        "--specs-dir",
        default="source_specs",
        help="Directory containing source specs (default: source_specs)"
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Treat warnings as errors"
    )
    args = parser.parse_args()
    
    specs_dir = Path(args.specs_dir)
    if not specs_dir.exists():
        print(f"❌ Source specs directory not found: {specs_dir}")
        sys.exit(1)
    
    # Find spec files to validate
    if args.source:
        # Find specific source
        spec_files = list(specs_dir.rglob(f"{args.source}.yaml"))
        if not spec_files:
            print(f"❌ Source spec not found: {args.source}")
            sys.exit(1)
    else:
        spec_files = list(specs_dir.rglob("*.yaml"))
    
    if not spec_files:
        print(f"❌ No YAML files found in {specs_dir}")
        sys.exit(1)
    
    print(f"Validating {len(spec_files)} source spec(s)...\n")
    
    # Check if lxml is available for XPath validation
    try:
        import lxml
        print("✓ lxml available - XPath validation enabled\n")
    except ImportError:
        print("⚠ lxml not installed - XPath syntax validation skipped")
        print("  Install with: pip install lxml\n")
    
    all_valid = True
    for spec_path in sorted(spec_files):
        relative_path = spec_path.relative_to(specs_dir)
        errors = validate_spec(spec_path)
        
        if errors:
            all_valid = False
            print(f"❌ {relative_path}")
            for error in errors:
                print(f"   - {error}")
        else:
            print(f"✓ {relative_path}")
    
    print()
    if all_valid:
        print("✓ All source specs are valid")
        sys.exit(0)
    else:
        print("❌ Some source specs have errors")
        sys.exit(1)


if __name__ == "__main__":
    main()
