"""Conservative retained-object charges, independent of wire payload quotas."""
import sys
from dataclasses import fields, is_dataclass


def retained_bytes(value, seen=None):
    seen = set() if seen is None else seen
    if id(value) in seen:
        return 0
    seen.add(id(value))
    size = sys.getsizeof(value)
    if is_dataclass(value) and not isinstance(value, type):
        if hasattr(value, '__dict__'):
            size += sys.getsizeof(value.__dict__)
        size += sum(retained_bytes(getattr(value, f.name), seen) for f in fields(value))
    elif isinstance(value, dict):
        size += sum(retained_bytes(k, seen)+retained_bytes(v, seen) for k,v in value.items())
    elif isinstance(value, (tuple, list, set, frozenset)):
        size += sum(retained_bytes(v, seen) for v in value)
    return size


def event_charge(sample):
    # Includes conservative per-entry allowance for FIFO, owner indices and
    # allocator/container growth; shared strings are deliberately overcharged.
    return retained_bytes(sample)+384
