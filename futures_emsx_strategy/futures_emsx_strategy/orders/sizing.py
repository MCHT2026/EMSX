"""Order slicing helpers for breaking large parent orders into child clips."""
from __future__ import annotations


def slice_order(total_qty: int, max_clip: int) -> list[int]:
    """Greedy slicer: produces clips of `max_clip` plus a remainder."""
    if total_qty <= 0:
        return []
    if max_clip <= 0:
        raise ValueError("max_clip must be positive")
    full = total_qty // max_clip
    remainder = total_qty - full * max_clip
    clips = [max_clip] * full
    if remainder:
        clips.append(remainder)
    return clips
