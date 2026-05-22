"""Pytest configuration. Adds the project root to sys.path so tests can
import `core`, `modules`, `config` regardless of where pytest is run from.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
