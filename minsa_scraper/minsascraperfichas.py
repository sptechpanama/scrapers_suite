"""Wrapper para ejecutar scrape_minsa sólo para fichas CTNI."""

from __future__ import annotations

import sys

from scrape_minsa import main


if __name__ == "__main__":
    extra_args = ["--mode", "fichas", *sys.argv[1:]]
    raise SystemExit(main(extra_args))
