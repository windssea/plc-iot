"""Deterministic reads: no holes, no split multi-register values."""
from dataclasses import dataclass


def width(point):
    return 2 if point.data_type in ('int32', 'uint32', 'float32') else 1


@dataclass(frozen=True)
class ReadBlock:
    area: str
    address: int
    count: int
    poll_interval_ms: int
    points: tuple


def plan_reads(points):
    blocks = []
    for point in sorted((p for p in points if p.enabled),
                        key=lambda p: (p.modbus.area, p.poll_interval_ms, p.modbus.address, p.point_id)):
        area, address = point.modbus.area, point.modbus.address
        end = address + width(point)
        limit = 2000 if area in ('coil', 'discrete_input') else 125
        old = blocks[-1] if blocks else None
        if (old and old.area == area and old.poll_interval_ms == point.poll_interval_ms
                and address <= old.address + old.count and end - old.address <= limit):
            blocks[-1] = ReadBlock(area, old.address, max(old.count, end - old.address),
                                  old.poll_interval_ms, old.points + (point,))
        else:
            blocks.append(ReadBlock(area, address, width(point), point.poll_interval_ms, (point,)))
    return tuple(blocks)
