"""
Validation module for the Orchestrator.

Handles two types of row count validation:

1. Go files: Separate control files that signal a data file is complete
   and provide expected row counts. Two formats supported:
   - Properties: key=value pairs (e.g., record_count=12345)
   - CSV: Comma-separated values with row count in a specific column

2. Trailer records: The last row of a data file contains metadata
   including expected row count. The trailer is removed before loading.
"""

from dataclasses import dataclass
from pathlib import Path
import fnmatch
import polars as pl

from orchestrator.config import GoFileConfig
from orchestrator.storage import Storage


@dataclass
class ValidationResult:
    """
    Result of a validation operation.
    
    Contains whether validation passed and details about what was checked.
    On failure, includes an error message explaining the mismatch.
    """
    valid: bool                         # Whether validation passed
    expected_rows: int | None = None    # Row count from control file/trailer
    actual_rows: int | None = None      # Actual data row count
    error: str | None = None            # Error message if validation failed


def extract_row_count_from_properties(content: bytes, key: str) -> int:
    """
    Extract row count from a properties-format go file.
    
    Properties format is key=value pairs, one per line:
        record_count=12345
        timestamp=2024-01-15T10:30:00Z
        status=complete
    
    Args:
        content: File contents as bytes
        key: The key name to look for (e.g., "record_count")
    
    Returns:
        The row count as an integer
    
    Raises:
        ValueError: If the key is not found in the file
    """
    for line in content.decode("utf-8").splitlines():
        line = line.strip()
        
        # Skip empty lines and comments
        if not line or line.startswith("#"):
            continue
        
        if "=" in line:
            k, v = line.split("=", 1)
            if k.strip() == key:
                return int(v.strip())
    
    raise ValueError(f"Key '{key}' not found in properties file")


def extract_row_count_from_csv(content: bytes, column: int) -> int:
    """
    Extract row count from a CSV-format go file.
    
    Expects a single-row CSV where one column contains the row count.
    Example: trades_20240115.csv,12345,SUCCESS
    
    Args:
        content: File contents as bytes
        column: Zero-indexed column number containing the row count
    
    Returns:
        The row count as an integer
    """
    # Read as CSV with no header, all strings
    df = pl.read_csv(content, has_header=False, infer_schema_length=0)
    
    # Get value from first row at specified column
    value = df.row(0)[column]
    return int(value)


def find_go_file(
    data_filename: str,
    go_config: GoFileConfig,
    available_files: list[str]
) -> str | None:
    """
    Find the matching go file for a data file.
    
    Matches based on the variable part of the filename (typically a date).
    For example:
        Data file: trades_20240115.csv (pattern: trades_*.csv)
        Go file:   trades_20240115.go  (pattern: trades_*.go)
    
    The matching is done by extracting the "identifier" portion (the part
    matched by *) and looking for a go file with the same identifier.
    
    Args:
        data_filename: The data filename (e.g., "trades_20240115.csv")
        go_config: Go file configuration with pattern
        available_files: List of all files in the landing directory
    
    Returns:
        Full path to the matching go file, or None if not found
    """
    # Extract the stem (filename without extension) of the data file
    data_stem = Path(data_filename).stem
    
    for filepath in available_files:
        filename = Path(filepath).name
        
        # Check if this file matches the go file pattern
        if fnmatch.fnmatch(filename, go_config.pattern):
            go_stem = Path(filename).stem
            
            # Extract identifier by comparing the parts
            # Assumes format like prefix_identifier.extension
            # e.g., trades_20240115.csv -> identifier is 20240115
            data_parts = data_stem.split("_")
            go_parts = go_stem.split("_")
            
            # Compare the last segment (usually the date/identifier)
            if len(data_parts) > 0 and len(go_parts) > 0:
                if data_parts[-1] == go_parts[-1]:
                    return filepath
    
    return None


def validate_with_go_file(
    go_path: str,
    storage: Storage,
    go_config: GoFileConfig,
    actual_rows: int
) -> ValidationResult:
    """
    Validate row count against a go file.
    
    Reads the go file, extracts the expected row count based on the
    configured format, and compares against the actual row count.
    
    Args:
        go_path: Path to the go file
        storage: Storage implementation for reading the file
        go_config: Configuration specifying format and field location
        actual_rows: Actual number of data rows to validate
    
    Returns:
        ValidationResult indicating success or failure with details
    """
    try:
        # Read go file contents
        content = storage.read_file(go_path)
        
        # Extract expected row count based on format
        if go_config.format == "properties":
            expected = extract_row_count_from_properties(
                content,
                go_config.row_count_key
            )
        elif go_config.format == "csv":
            expected = extract_row_count_from_csv(
                content,
                go_config.row_count_column
            )
        else:
            return ValidationResult(
                valid=False,
                error=f"Unknown go file format: {go_config.format}"
            )
        
        # Compare counts
        if actual_rows == expected:
            return ValidationResult(
                valid=True,
                expected_rows=expected,
                actual_rows=actual_rows
            )
        else:
            return ValidationResult(
                valid=False,
                expected_rows=expected,
                actual_rows=actual_rows,
                error=f"Row count mismatch: expected {expected}, got {actual_rows}"
            )
    
    except Exception as e:
        return ValidationResult(
            valid=False,
            error=f"Failed to parse go file: {e}"
        )


def process_with_trailer(
    df: pl.DataFrame,
    row_count_column: int
) -> tuple[pl.DataFrame, ValidationResult]:
    """
    Process a DataFrame with a trailer record.
    
    The trailer record is the last row of the file and contains metadata
    including an expected row count. This function:
    1. Extracts the expected row count from the trailer
    2. Removes the trailer row from the data
    3. Validates that the actual data row count matches expected
    
    Args:
        df: DataFrame including the trailer row
        row_count_column: Zero-indexed column containing row count in trailer
    
    Returns:
        Tuple of (data_without_trailer, validation_result)
    """
    try:
        # Get expected count from last row
        last_row = df.row(-1)
        expected = int(last_row[row_count_column])
        
        # Remove trailer row
        data = df.head(-1)
        actual = len(data)
        
        # Validate count
        if actual == expected:
            return data, ValidationResult(
                valid=True,
                expected_rows=expected,
                actual_rows=actual
            )
        else:
            return data, ValidationResult(
                valid=False,
                expected_rows=expected,
                actual_rows=actual,
                error=f"Row count mismatch: expected {expected}, got {actual}"
            )
    
    except Exception as e:
        # Return original data minus last row, with error
        return df.head(-1), ValidationResult(
            valid=False,
            error=f"Failed to process trailer: {e}"
        )
