#!/usr/bin/env python3
"""One-off: archive 23 old duplicate-prefix files out of TEST_INPUT and ../input.

Moves files whose prefix is 31-53 and whose stem is NOT present in the current
``_preset_cases()`` into ``.archive_old_named_graphs/`` so they can be restored
later if needed.
"""
from __future__ import annotations

import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from generate_custom_test_suite import _preset_cases  # noqa: E402

HERE = Path(__file__).resolve().parent
TEST_INPUT = HERE / "TEST_INPUT"
INPUT_DIR = HERE.parent / "input"
ARCHIVE_YAML = HERE / ".archive_old_named_graphs" / "TEST_INPUT"
ARCHIVE_JSON = HERE / ".archive_old_named_graphs" / "input"

ARCHIVE_YAML.mkdir(parents=True, exist_ok=True)
ARCHIVE_JSON.mkdir(parents=True, exist_ok=True)

preset_names = {c.name for c in _preset_cases()}

moved_yaml: list[str] = []
moved_json: list[str] = []
for yaml_path in sorted(TEST_INPUT.glob("*.yaml")):
    stem = yaml_path.stem
    prefix = stem.split("_", 1)[0]
    if not prefix.isdigit():
        continue
    n = int(prefix)
    if 31 <= n <= 53 and stem not in preset_names:
        dest = ARCHIVE_YAML / yaml_path.name
        shutil.move(str(yaml_path), str(dest))
        moved_yaml.append(yaml_path.name)

        jp = INPUT_DIR / f"{stem}.json"
        if jp.exists():
            jdest = ARCHIVE_JSON / jp.name
            shutil.move(str(jp), str(jdest))
            moved_json.append(jp.name)

print(f"Archived YAMLs: {len(moved_yaml)}")
for n in moved_yaml:
    print(f"  {n}")
print(f"Archived JSONs: {len(moved_json)}")
for n in moved_json:
    print(f"  {n}")
