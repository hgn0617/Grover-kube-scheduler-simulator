#!/usr/bin/env python3
"""Build JSON conflict graphs for every YAML under TEST_INPUT/.

Each generated YAML encodes pod anti-affinity as ``app: app-<i>`` labels that
are forbidden via ``matchExpressions`` of the form::

  - key: app
    operator: In
    values: ["app-2", "app-3"]

We recover the conflict graph by parsing those ``values`` lists per Pod.
Output filename matches ``run_test.sh`` expectation:

    ../input/<batch_name>.json   (relative to this script)

where ``batch_name`` is the YAML stem.

Safe to re-run: overwrites existing JSON files atomically.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent
TEST_INPUT_DIR = HERE / "TEST_INPUT"
OUTPUT_DIR = REPO_ROOT / "input"

POD_NAME_RE = re.compile(r"name:\s*pod-(\d+)")
APP_LABEL_RE = re.compile(r"app:\s*app-(\d+)")
VALUES_RE = re.compile(r"values:\s*\[([^\]]+)\]")
APP_ID_RE = re.compile(r"app-(\d+)")


def _extract_graph(yaml_text: str) -> tuple[int, list[tuple[int, int]]]:
    """Parse a Kubernetes YAML and recover the pod-level conflict graph.

    Returns
    -------
    (n_pods, edges) where pod ids are 0-indexed and edges are undirected
    ``(u, v)`` pairs with ``u < v``.
    """
    # Split on YAML document boundary.
    docs = re.split(r"^---\s*$", yaml_text, flags=re.MULTILINE)
    pods: dict[int, set[int]] = {}

    for doc in docs:
        if "kind: Pod" not in doc:
            continue
        name_match = POD_NAME_RE.search(doc)
        app_match = APP_LABEL_RE.search(doc)
        if not name_match or not app_match:
            continue
        pod_app = int(app_match.group(1))

        # podAntiAffinity values: ["app-X", "app-Y", ...]
        neighbors: set[int] = set()
        for values_str in VALUES_RE.findall(doc):
            for tok in APP_ID_RE.findall(values_str):
                neighbors.add(int(tok))
        neighbors.discard(pod_app)
        pods[pod_app] = neighbors

    if not pods:
        return 0, []

    # Pod app IDs are 1-indexed in generated YAMLs; normalize to 0-indexed.
    sorted_ids = sorted(pods.keys())
    id_map = {old: new for new, old in enumerate(sorted_ids)}
    n = len(sorted_ids)

    edges: set[tuple[int, int]] = set()
    for old, nbrs in pods.items():
        u = id_map[old]
        for nb in nbrs:
            if nb not in id_map:
                continue
            v = id_map[nb]
            if u == v:
                continue
            a, b = (u, v) if u < v else (v, u)
            edges.add((a, b))

    return n, sorted(edges)


def _normalize_edges_for_compare(edges) -> set[tuple[int, int]]:
    out: set[tuple[int, int]] = set()
    for e in edges or []:
        if not isinstance(e, (list, tuple)) or len(e) != 2:
            continue
        u, v = int(e[0]), int(e[1])
        if u == v:
            continue
        out.add((u, v) if u < v else (v, u))
    return out


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    yamls = sorted(TEST_INPUT_DIR.glob("*.yaml"))
    if not yamls:
        print(f"[ERROR] no YAML files under {TEST_INPUT_DIR}")
        return 1

    written = 0
    preserved = 0
    empty = 0
    mismatched = 0
    for yaml_path in yamls:
        try:
            text = yaml_path.read_text(encoding="utf-8")
        except Exception as exc:
            print(f"[skip] {yaml_path.name}: read error {exc}")
            continue

        n, edges = _extract_graph(text)
        if n == 0:
            print(f"[warn] {yaml_path.name}: no pods parsed; skipping")
            empty += 1
            continue

        json_path = OUTPUT_DIR / f"{yaml_path.stem}.json"

        # If a richer JSON already exists and its graph structure matches the
        # YAML, preserve it verbatim (to keep metadata such as k_opt / k_greedy).
        if json_path.exists():
            try:
                existing = json.loads(json_path.read_text(encoding="utf-8"))
            except Exception:
                existing = None
            if (
                isinstance(existing, dict)
                and int(existing.get("nodes", -1)) == n
                and _normalize_edges_for_compare(existing.get("edges")) == set(edges)
            ):
                preserved += 1
                continue
            # Structure drifted: print a warning so the user can investigate.
            if existing is not None:
                mismatched += 1
                print(
                    f"[overwrite] {json_path.name}: existing JSON edges differ from YAML; rewriting"
                )

        payload = {
            "name": yaml_path.stem,
            "nodes": n,
            "edges": [list(e) for e in edges],
        }
        tmp_path = json_path.with_suffix(".json.tmp")
        tmp_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        tmp_path.replace(json_path)
        written += 1

    print(
        f"[build_input_graphs] wrote={written} preserved={preserved} "
        f"mismatched={mismatched} empty={empty}"
    )
    print(f"  output: {OUTPUT_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
