"""Compatibility entry point for the shared, source-tree core validator."""
from tools import _source_path  # noqa: F401
from plcnext_iot.contracts import CONTRACT_DIR, Issue, topic_rules, validate_message

__all__ = ["CONTRACT_DIR", "Issue", "topic_rules", "validate_message"]
