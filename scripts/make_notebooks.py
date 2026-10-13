"""Generate notebooks/<name>.ipynb from jobs/<name>.py so both always share the same logic.

The jobs use the "percent" format: `# %%` starts a code cell, `# %% [markdown]` a markdown cell.
Usage: python scripts/make_notebooks.py
"""

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
NAMES = ["bronze_ingest", "silver_transform", "gold_agg", "prepare_bronze_manual", "silver_post_hist_manual"]


def to_cells(source: str):
    cells, kind, buffer = [], None, []

    def flush():
        if kind is None:
            return
        lines = list(buffer)
        while lines and not lines[-1].strip():
            lines.pop()
        while lines and not lines[0].strip():
            lines.pop(0)
        if kind == "markdown":
            text = [line[2:] if line.startswith("# ") else line.lstrip("#") for line in lines]
            cells.append({"cell_type": "markdown", "metadata": {}, "source": to_source(text)})
        elif lines:
            cells.append(
                {"cell_type": "code", "metadata": {}, "execution_count": None, "outputs": [], "source": to_source(lines)}
            )

    for line in source.splitlines():
        if line.startswith("# %%"):
            flush()
            kind = "markdown" if "[markdown]" in line else "code"
            buffer = []
        else:
            buffer.append(line)
    flush()
    return cells


def to_source(lines):
    return [line + "\n" for line in lines[:-1]] + [lines[-1]] if lines else []


for name in NAMES:
    cells = to_cells((ROOT / "jobs" / f"{name}.py").read_text())
    notebook = {
        "cells": cells,
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }
    for index, cell in enumerate(cells):
        cell["id"] = f"{name}-{index}"
    (ROOT / "notebooks" / f"{name}.ipynb").write_text(json.dumps(notebook, indent=1, ensure_ascii=False) + "\n")
    print(f"notebooks/{name}.ipynb ({len(cells)} cells)")
