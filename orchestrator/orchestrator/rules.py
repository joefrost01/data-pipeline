"""Rule evaluation engine for source spec validation rules.

Evaluates validation rules defined in source specs against row data.
Rules are simple expressions like "quantity > 0" or "side in ('BUY', 'SELL')".
"""

import operator
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

import structlog

log = structlog.get_logger()


@dataclass
class RuleResult:
    """Result of evaluating a rule against a row."""
    passed: bool
    rule: str
    message: str | None = None


class RuleEvaluator:
    """Evaluates validation rules against row data.
    
    Supported rule syntax:
        - field is not null
        - field is null
        - field > value
        - field >= value
        - field < value
        - field <= value
        - field = value
        - field != value
        - field in ('val1', 'val2', ...)
        - field not in ('val1', 'val2', ...)
        - field matches 'regex'
        - field <= current_timestamp()
    """
    
    # Pattern matchers for different rule types
    PATTERNS = [
        # "field is not null"
        (r"^(\w+)\s+is\s+not\s+null$", "_eval_is_not_null"),
        # "field is null"
        (r"^(\w+)\s+is\s+null$", "_eval_is_null"),
        # "field in ('val1', 'val2')"
        (r"^(\w+)\s+in\s+\(([^)]+)\)$", "_eval_in"),
        # "field not in ('val1', 'val2')"
        (r"^(\w+)\s+not\s+in\s+\(([^)]+)\)$", "_eval_not_in"),
        # "field matches 'regex'"
        (r"^(\w+)\s+matches\s+'([^']+)'$", "_eval_matches"),
        # "field <= current_timestamp()"
        (r"^(\w+)\s*(<=?|>=?)\s*current_timestamp\(\)$", "_eval_timestamp_compare"),
        # "field op value" (comparisons)
        (r"^(\w+)\s*(<=?|>=?|!=|=)\s*(.+)$", "_eval_comparison"),
    ]
    
    def __init__(self) -> None:
        self._compiled_patterns = [
            (re.compile(pattern, re.IGNORECASE), method_name)
            for pattern, method_name in self.PATTERNS
        ]
    
    def evaluate(self, rule: str, row: dict[str, Any], row_num: int) -> RuleResult:
        """Evaluate a single rule against a row.
        
        Args:
            rule: Rule expression string from source spec
            row: Dictionary of field name -> value
            row_num: Row number for error messages
            
        Returns:
            RuleResult indicating pass/fail and any error message
        """
        rule = rule.strip()
        
        for pattern, method_name in self._compiled_patterns:
            match = pattern.match(rule)
            if match:
                method = getattr(self, method_name)
                try:
                    return method(rule, row, row_num, match)
                except Exception as e:
                    return RuleResult(
                        passed=False,
                        rule=rule,
                        message=f"Rule evaluation error at row {row_num}: {e}",
                    )
        
        log.warning("unrecognised_rule", rule=rule)
        return RuleResult(
            passed=True,  # Don't fail on unrecognised rules, but log warning
            rule=rule,
            message=f"Unrecognised rule syntax: {rule}",
        )
    
    def evaluate_all(
        self, rules: list[dict], row: dict[str, Any], row_num: int
    ) -> list[RuleResult]:
        """Evaluate all rules against a row.
        
        Args:
            rules: List of rule dicts with 'rule' and 'severity' keys
            row: Dictionary of field name -> value
            row_num: Row number for error messages
            
        Returns:
            List of RuleResults for failed rules only
        """
        failures = []
        for rule_spec in rules:
            rule_str = rule_spec.get("rule", "")
            result = self.evaluate(rule_str, row, row_num)
            if not result.passed:
                failures.append(result)
        return failures
    
    def _eval_is_not_null(
        self, rule: str, row: dict, row_num: int, match: re.Match
    ) -> RuleResult:
        field = match.group(1)
        value = row.get(field)
        passed = value is not None and value != ""
        return RuleResult(
            passed=passed,
            rule=rule,
            message=None if passed else f"Field '{field}' is null at row {row_num}",
        )
    
    def _eval_is_null(
        self, rule: str, row: dict, row_num: int, match: re.Match
    ) -> RuleResult:
        field = match.group(1)
        value = row.get(field)
        passed = value is None or value == ""
        return RuleResult(
            passed=passed,
            rule=rule,
            message=None if passed else f"Field '{field}' is not null at row {row_num}",
        )
    
    def _eval_in(
        self, rule: str, row: dict, row_num: int, match: re.Match
    ) -> RuleResult:
        field = match.group(1)
        values_str = match.group(2)
        allowed = self._parse_value_list(values_str)
        value = row.get(field)
        passed = value in allowed
        return RuleResult(
            passed=passed,
            rule=rule,
            message=None if passed else (
                f"Field '{field}' value '{value}' not in {allowed} at row {row_num}"
            ),
        )
    
    def _eval_not_in(
        self, rule: str, row: dict, row_num: int, match: re.Match
    ) -> RuleResult:
        field = match.group(1)
        values_str = match.group(2)
        disallowed = self._parse_value_list(values_str)
        value = row.get(field)
        passed = value not in disallowed
        return RuleResult(
            passed=passed,
            rule=rule,
            message=None if passed else (
                f"Field '{field}' value '{value}' is disallowed at row {row_num}"
            ),
        )
    
    def _eval_matches(
        self, rule: str, row: dict, row_num: int, match: re.Match
    ) -> RuleResult:
        field = match.group(1)
        pattern = match.group(2)
        value = row.get(field)
        if value is None:
            passed = False
        else:
            passed = bool(re.match(pattern, str(value)))
        return RuleResult(
            passed=passed,
            rule=rule,
            message=None if passed else (
                f"Field '{field}' value '{value}' doesn't match pattern at row {row_num}"
            ),
        )
    
    def _eval_timestamp_compare(
        self, rule: str, row: dict, row_num: int, match: re.Match
    ) -> RuleResult:
        field = match.group(1)
        op_str = match.group(2)
        value = row.get(field)
        
        if value is None:
            return RuleResult(passed=True, rule=rule)  # Null handled by nullable check
        
        now = datetime.now(timezone.utc)
        
        # Ensure value is datetime
        if isinstance(value, str):
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        
        op_func = self._get_operator(op_str)
        passed = op_func(value, now)
        
        return RuleResult(
            passed=passed,
            rule=rule,
            message=None if passed else (
                f"Field '{field}' timestamp check failed at row {row_num}"
            ),
        )
    
    def _eval_comparison(
        self, rule: str, row: dict, row_num: int, match: re.Match
    ) -> RuleResult:
        field = match.group(1)
        op_str = match.group(2)
        compare_value_str = match.group(3).strip()
        
        value = row.get(field)
        if value is None:
            return RuleResult(passed=True, rule=rule)  # Null handled by nullable check
        
        # Parse comparison value to match field type
        compare_value = self._parse_literal(compare_value_str, type(value))
        op_func = self._get_operator(op_str)
        
        try:
            passed = op_func(value, compare_value)
        except TypeError:
            passed = False
        
        return RuleResult(
            passed=passed,
            rule=rule,
            message=None if passed else (
                f"Field '{field}' comparison '{value} {op_str} {compare_value}' "
                f"failed at row {row_num}"
            ),
        )
    
    def _parse_value_list(self, values_str: str) -> set[Any]:
        """Parse a comma-separated list of values like "'BUY', 'SELL'"."""
        values = set()
        for part in values_str.split(","):
            part = part.strip()
            if part.startswith("'") and part.endswith("'"):
                values.add(part[1:-1])
            elif part.startswith('"') and part.endswith('"'):
                values.add(part[1:-1])
            else:
                # Try numeric
                try:
                    if "." in part:
                        values.add(float(part))
                    else:
                        values.add(int(part))
                except ValueError:
                    values.add(part)
        return values
    
    def _parse_literal(self, value_str: str, target_type: type) -> Any:
        """Parse a literal value string to the target type."""
        value_str = value_str.strip()
        
        # Remove quotes if present
        if (value_str.startswith("'") and value_str.endswith("'")) or \
           (value_str.startswith('"') and value_str.endswith('"')):
            return value_str[1:-1]
        
        # Try to match target type
        if target_type == int:
            return int(value_str)
        elif target_type == float:
            return float(value_str)
        elif target_type.__name__ == "Decimal":
            from decimal import Decimal
            return Decimal(value_str)
        else:
            return value_str
    
    def _get_operator(self, op_str: str) -> Callable[[Any, Any], bool]:
        """Get operator function from string."""
        ops = {
            ">": operator.gt,
            ">=": operator.ge,
            "<": operator.lt,
            "<=": operator.le,
            "=": operator.eq,
            "!=": operator.ne,
        }
        return ops[op_str]
