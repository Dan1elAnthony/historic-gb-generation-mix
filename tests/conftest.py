"""Pytest configuration shared across the test suite."""

from __future__ import annotations

import sys
from pathlib import Path

# Ensure the project root is on sys.path so ``import ingest`` works when running
# the test suite without installing the package.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
