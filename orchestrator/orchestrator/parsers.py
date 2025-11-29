"""File parsers for different source formats."""

import csv
import json
from abc import ABC, abstractmethod
from typing import Any

import structlog

log = structlog.get_logger()


class Parser(ABC):
    """Base parser interface."""
    
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


class CsvParser(Parser):
    """Parser for CSV files."""
    
    def parse_and_validate(
        self, file_path: str, spec: dict
    ) -> tuple[list[dict], list[dict]]:
        valid_rows = []
        quarantined = []
        schema = {field["name"]: field for field in spec["schema"]}
        
        with open(file_path, newline="") as f:
            reader = csv.DictReader(f)
            for row_num, row in enumerate(reader, start=2):  # Start at 2 (header is 1)
                is_valid, cleaned, reason = self._validate_row(row, schema, row_num)
                if is_valid:
                    valid_rows.append(cleaned)
                else:
                    quarantined.append({
                        "row_number": row_num,
                        "raw_content": row,
                        "failure_reason": reason,
                    })
        
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
            if is_valid:
                valid_rows.append(cleaned)
            else:
                quarantined.append({
                    "row_number": row_num,
                    "raw_content": row,
                    "failure_reason": reason,
                })
        
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
        
        return True, cleaned, None


class XmlParser(Parser):
    """Parser for XML files."""
    
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
        
        # Parse XML
        context = etree.iterparse(file_path, events=("end",), tag=row_element)
        
        for row_num, (event, elem) in enumerate(context, start=1):
            row = {}
            for field in spec["schema"]:
                xpath = field.get("xpath")
                if xpath:
                    values = elem.xpath(xpath, namespaces=namespaces)
                    row[field["name"]] = values[0] if values else None
                else:
                    row[field["name"]] = None
            
            is_valid, cleaned, reason = self._validate_row(row, schema, row_num)
            if is_valid:
                valid_rows.append(cleaned)
            else:
                quarantined.append({
                    "row_number": row_num,
                    "raw_content": etree.tostring(elem, encoding="unicode"),
                    "failure_reason": reason,
                })
            
            # Clear element to save memory
            elem.clear()
        
        return valid_rows, quarantined
    
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
                cleaned[field_name] = value
        
        return True, cleaned, None


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
