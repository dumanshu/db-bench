#!/usr/bin/env python3
"""Thin redirect: ``python3 -m aurora.benchmark`` -> unified CLI."""
import os as _os, sys as _sys                       # noqa: E401,E402
_sys.path.insert(0, _os.path.join(_os.path.dirname(__file__), ".."))

import sys
sys.argv.insert(1, "--server-type")
sys.argv.insert(2, "aurora")

from common.benchmark import main  # noqa: E402

if __name__ == "__main__":
    main()
