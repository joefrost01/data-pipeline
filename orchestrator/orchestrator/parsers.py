"""File parsers for different source formats."""

import csv
import json
from abc import ABC, abstractmethod
from typing import Any

import structlog

from orchestrator.orchestrator.rules import RuleEvaluator

log = structlog.get_logger()


class Parser(ABC):
    """Base parser interface."""
    
    def __init__(self) -> None:
        self.rule_evaluator = RuleEvaluator()
    
    @abstractmethod
    def parse_and_validate(
        self, file_path: str, spec: dict
    ) -> tuple[list[dict], list[dict]]:
        """
        Parse file and validate rows.
        
        Returns:
            Tuple of (valid_rows, quarantined_rows)
        """
        pass
    
    def _apply_validation_rules(
        self, row: dict, spec: dict, row_num: int
    ) -> tuple[bool, str | None]:
        """Apply validation rules from spec to a row.
        
        Returns:
            Tuple of (passed, failure_reason)
        """
        validation = spec.get("validation", {})
        row_rules = validation.get("row_level", [])
        
        if not row_rules:
            return True, None
        
        failures = self.rule_evaluator.evaluate_all(row_rules, row, row_num)
        
        # Filter to only error-severity failures
        error_failures = [
            f for f in failures 
            if any(r.get("severity") == "error" and r.get("rule") == f.rule 
                   for r in row_rules)
        ]
        
        if error_failures:
            messages = [f.message for f in error_failures if f.message]
            return False, "; ".join(messages)
        
        # Log warnings but don't fail
        warning_failures = [f for f in failures if f not in error_failures]
        for failure in warning_failures:
            log.warning("validation_warning", rule=failure.rule, message=failure.message)
        
        return True, None


class CsvParser(Parser):
    """Parser for CSV files."""
    
    def parse_and_validate(
        self, file_path: str, spec: dict
    ) -> tuple[list[dict], list[dict]]:
        valid_rows = []
        quarantined = []
        schema = {field["name"]: field for field in spec["schema"]}
        
        # Get CSV-specific config
        source_config = spec.get("source", {})
        delimiter = source_config.get("delimiter", ",")
        encoding = source_config.get("encoding", "utf-8")
        
        with open(file_path, newline="", encoding=encoding) as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            for row_num, row in enumerate(reader, start=2):  # Start at 2 (header is 1)
                # Schema validation (types, nullability)
                is_valid, cleaned, reason = self._validate_row(row, schema, row_num)
                
                if not is_valid:
                    quarantined.append({
                        "row_number": row_num,
                        "raw_content": row,
                        "failure_reason": reason,
                    })
                    continue
                
                # Apply custom validation rules from spec
                rules_passed, rule_reason = self._apply_validation_rules(
                    cleaned, spec, row_num
                )
                
                if not rules_passed:
                    quarantined.append({
                        "row_number": row_num,
                        "raw_content": row,
                        "failure_reason": rule_reason,
                    })
                    continue
                
                valid_rows.append(cleaned)
        
        return valid_rows, quarantined
    
    def _validate_row(
        self, row: dict, schema: dict, row_num: int
    ) -> tuple[bool, dict, str | None]:
        """Validate and type-convert a single row."""
        cleaned = {}
        
        for field_name, field_spec in schema.items():
            value = row.get(field_name)
            
            # Check nullable
            if value is None or value == "":
                if not field_spec.get("nullable", True):
                    return False, {}, f"Required field '{field_name}' is null at row {row_num}"
                cleaned[field_name] = None
                continue
            
            # Type conversion
            try:
                cleaned[field_name] = self._convert_type(value, field_spec["type"])
            except (ValueError, TypeError) as e:
                return False, {}, f"Type conversion failed for '{field_name}' at row {row_num}: {e}"
            
            # Check allowed_values if specified
            allowed = field_spec.get("allowed_values")
            if allowed and cleaned[field_name] not in allowed:
                return False, {}, (
                    f"Field '{field_name}' value '{cleaned[field_name]}' "
                    f"not in allowed values {allowed} at row {row_num}"
                )
            
            # Check min_value/max_value for numeric fields
            min_val = field_spec.get("min_value")
            max_val = field_spec.get("max_value")
            if min_val is not None and cleaned[field_name] < min_val:
                return False, {}, (
                    f"Field '{field_name}' value {cleaned[field_name]} "
                    f"below minimum {min_val} at row {row_num}"
                )
            if max_val is not None and cleaned[field_name] > max_val:
                return False, {}, (
                    f"Field '{field_name}' value {cleaned[field_name]} "
                    f"above maximum {max_val} at row {row_num}"
                )
        
        return True, cleaned, None
    
    def _convert_type(self, value: str, type_name: str) -> Any:
        """Convert string value to specified type."""
        if type_name == "STRING":
            return value
        elif type_name == "INT64":
            return int(value)
        elif type_name == "FLOAT64":
            return float(value)
        elif type_name == "NUMERIC":
            from decimal import Decimal
            return Decimal(value)
        elif type_name == "BOOL":
            return value.lower() in ("true", "1", "yes")
        elif type_name == "TIMESTAMP":
            from datetime import datetime
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        elif type_name == "DATE":
            from datetime import date
            return date.fromisoformat(value)
        else:
            return value


class JsonParser(Parser):
    """Parser for JSON/JSONL files."""
    
    def parse_and_validate(
        self, file_path: str, spec: dict
    ) -> tuple[list[dict], list[dict]]:
        valid_rows = []
        quarantined = []
        schema = {field["name"]: field for field in spec["schema"]}
        
        with open(file_path) as f:
            # Try JSONL first
            content = f.read()
            if content.strip().startswith("["):
                rows = json.loads(content)
            else:
                rows = [json.loads(line) for line in content.strip().split("\n") if line]
        
        for row_num, row in enumerate(rows, start=1):
            is_valid, cleaned, reason = self._validate_row(row, schema, row_num)
            
            if not is_valid:
                quarantined.append({
                    "row_number": row_num,
                    "raw_content": row,
                    "failure_reason": reason,
                })
                continue
            
            # Apply custom validation rules
            rules_passed, rule_reason = self._apply_validation_rules(
                cleaned, spec, row_num
            )
            
            if not rules_passed:
                quarantined.append({
                    "row_number": row_num,
                    "raw_content": row,
                    "failure_reason": rule_reason,
                })
                continue
            
            valid_rows.append(cleaned)
        
        return valid_rows, quarantined
    
    def _validate_row(
        self, row: dict, schema: dict, row_num: int
    ) -> tuple[bool, dict, str | None]:
        """Validate a single row."""
        cleaned = {}
        
        for field_name, field_spec in schema.items():
            value = row.get(field_name)
            
            if value is None:
                if not field_spec.get("nullable", True):
                    return False, {}, f"Required field '{field_name}' is null at row {row_num}"
                cleaned[field_name] = None
            else:
                cleaned[field_name] = value
                
                # Check allowed_values
                allowed = field_spec.get("allowed_values")
                if allowed and value not in allowed:
                    return False, {}, (
                        f"Field '{field_name}' value '{value}' "
                        f"not in allowed values {allowed} at row {row_num}"
                    )
        
        return True, cleaned, None


class XmlParser(Parser):
    """Parser for XML files.
    
    IMPORTANT: Namespace handling behaviour:
    - When row_element is namespaced (e.g., 'ns:Trade'), only elements in that 
      exact namespace will be matched.
    - When row_element is not namespaced, elements are matched by local name only,
      which could match elements in ANY namespace with that local name.
    - For strict namespace enforcement, always use namespaced element names.
    """
    
    def parse_and_validate(
        self, file_path: str, spec: dict
    ) -> tuple[list[dict], list[dict]]:
        from lxml import etree
        
        valid_rows = []
        quarantined = []
        
        xml_config = spec.get("xml_config", {})
        namespaces = xml_config.get("namespaces", {})
        row_element = spec["source"]["row_element"]
        schema = {field["name"]: field for field in spec["schema"]}
        
        # Determine if we're doing strict namespace matching
        strict_namespace = ":" in row_element and row_element.split(":")[0] in namespaces
        
        # Parse XML with iterparse for memory efficiency
        context = etree.iterparse(file_path, events=("end",))
        
        row_num = 0
        for event, elem in context:
            # Check if this element matches our row element
            if not self._element_matches(elem, row_element, namespaces, strict_namespace):
                continue
            
            row_num += 1
            row = {}
            for field in spec["schema"]:
                xpath = field.get("xpath")
                if xpath:
                    values = elem.xpath(xpath, namespaces=namespaces)
                    row[field["name"]] = values[0] if values else None
                else:
                    row[field["name"]] = None
            
            is_valid, cleaned, reason = self._validate_row(row, schema, row_num)
            
            if not is_valid:
                quarantined.append({
                    "row_number": row_num,
                    "raw_content": etree.tostring(elem, encoding="unicode"),
                    "failure_reason": reason,
                })
            else:
                # Apply custom validation rules
                rules_passed, rule_reason = self._apply_validation_rules(
                    cleaned, spec, row_num
                )
                
                if not rules_passed:
                    quarantined.append({
                        "row_number": row_num,
                        "raw_content": etree.tostring(elem, encoding="unicode"),
                        "failure_reason": rule_reason,
                    })
                else:
                    valid_rows.append(cleaned)
            
            # Clear element to save memory
            elem.clear()
            # Also clear preceding siblings
            while elem.getprevious() is not None:
                del elem.getparent()[0]
        
        return valid_rows, quarantined
    
    def _build_tag_matcher(self, row_element: str, namespaces: dict) -> str:
        """Build a tag string that works with lxml for namespaced elements.
        
        Converts 'ns:Element' to '{http://namespace.uri}Element' (Clark notation).
        """
        if ":" in row_element:
            prefix, local = row_element.split(":", 1)
            if prefix in namespaces:
                return f"{{{namespaces[prefix]}}}{local}"
        return row_element
    
    def _element_matches(
        self, 
        elem: Any, 
        row_element: str, 
        namespaces: dict,
        strict_namespace: bool = True
    ) -> bool:
        """Check if an element matches the expected row element.
        
        Args:
            elem: The lxml element to check
            row_element: The expected element name (e.g., 'ns:Trade' or 'Trade')
            namespaces: Namespace prefix to URI mapping
            strict_namespace: If True, require exact namespace match. If False,
                            fall back to local name matching for non-namespaced specs.
        
        Returns:
            True if element matches, False otherwise.
        """
        # Build the expected tag in Clark notation
        expected_tag = self._build_tag_matcher(row_element, namespaces)
        
        # Exact match (including namespace)
        if elem.tag == expected_tag:
            return True
        
        # If strict namespace mode and we didn't get an exact match, fail
        if strict_namespace:
            return False
        
        # Fallback: local name match only (for non-namespaced element specs)
        # WARNING: This could match unintended elements in multi-namespace documents
        local_name = row_element.split(":")[-1] if ":" in row_element else row_element
        elem_local = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
        
        return elem_local == local_name
    
    def _validate_row(
        self, row: dict, schema: dict, row_num: int
    ) -> tuple[bool, dict, str | None]:
        """Validate a single row."""
        cleaned = {}
        
        for field_name, field_spec in schema.items():
            value = row.get(field_name)
            
            if value is None or value == "":
                if not field_spec.get("nullable", True):
                    return False, {}, f"Required field '{field_name}' is null at row {row_num}"
                cleaned[field_name] = None
            else:
                # Type conversion for XML (everything comes as string)
                try:
                    cleaned[field_name] = self._convert_type(value, field_spec["type"])
                except (ValueError, TypeError) as e:
                    return False, {}, f"Type conversion failed for '{field_name}' at row {row_num}: {e}"
                
                # Check allowed_values
                allowed = field_spec.get("allowed_values")
                if allowed and cleaned[field_name] not in allowed:
                    return False, {}, (
                        f"Field '{field_name}' value '{cleaned[field_name]}' "
                        f"not in allowed values {allowed} at row {row_num}"
                    )
        
        return True, cleaned, None
    
    def _convert_type(self, value: str, type_name: str) -> Any:
        """Convert string value to specified type."""
        if type_name == "STRING":
            return value
        elif type_name == "INT64":
            return int(value)
        elif type_name == "FLOAT64":
            return float(value)
        elif type_name == "NUMERIC":
            from decimal import Decimal
            return Decimal(value)
        elif type_name == "BOOL":
            return value.lower() in ("true", "1", "yes")
        elif type_name == "TIMESTAMP":
            from datetime import datetime
            # Handle various timestamp formats
            if "T" in value:
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
            else:
                # Try common formats
                for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d"]:
                    try:
                        return datetime.strptime(value, fmt)
                    except ValueError:
                        continue
                raise ValueError(f"Cannot parse timestamp: {value}")
        elif type_name == "DATE":
            from datetime import date
            return date.fromisoformat(value)
        else:
            return value


def get_parser(spec: dict) -> Parser:
    """Get appropriate parser for source specification."""
    format_type = spec["source"]["format"].lower()
    
    parsers = {
        "csv": CsvParser,
        "json": JsonParser,
        "jsonl": JsonParser,
        "xml": XmlParser,
    }
    
    parser_class = parsers.get(format_type)
    if parser_class is None:
        raise ValueError(f"Unsupported format: {format_type}")
    
    return parser_class()
