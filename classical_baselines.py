#!/usr/bin/env python3
"""
Compute classical coloring baselines (Greedy Largest-First / DSATUR) for a JSON graph.

Output format (default): two integers separated by a space: "<k_greedy> <k_dsatur>".
This is intended to be consumed by run_test.sh when writing results/benchmark_report.csv.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def _build_adj(n: int, edges: list[list[int]]) -> list[set[int]]:
    adj: list[set[int]] = [set() for _ in range(n)]
    for u, v in edges:
        if u == v:
            continue
        adj[u].add(v)
        adj[v].add(u)
    return adj


def greedy_largest_first(n: int, adj: list[set[int]]) -> int:
    if n <= 0:
        return 0
    order = sorted(range(n), key=lambda i: len(adj[i]), reverse=True)
    color: list[int | None] = [None] * n
    for u in order:
        used = {color[v] for v in adj[u] if color[v] is not None}
        c = 0
        while c in used:
            c += 1
        color[u] = c
    return (max(color) + 1) if any(c is not None for c in color) else 0


def dsatur(n: int, adj: list[set[int]]) -> int:
    if n <= 0:
        return 0
    color: list[int | None] = [None] * n
    uncolored = set(range(n))
    sat: list[int] = [0] * n
    neighbor_colors: list[set[int]] = [set() for _ in range(n)]

    def update(v: int) -> None:
        neighbor_colors[v] = {color[u] for u in adj[v] if color[u] is not None}  # type: ignore[misc]
        sat[v] = len(neighbor_colors[v])

    for v in range(n):
        update(v)

    while uncolored:
        v = max(uncolored, key=lambda x: (sat[x], len(adj[x]), x))
        used = neighbor_colors[v]
        c = 0
        while c in used:
            c += 1
        color[v] = c
        uncolored.remove(v)
        for u in adj[v]:
            if u in uncolored:
                update(u)

    return (max(color) + 1) if any(c is not None for c in color) else 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", type=Path, required=True)
    args = parser.parse_args()

    data = json.loads(args.json.read_text(encoding="utf-8"))
    n = int(data["nodes"])
    edges = data.get("edges") or []
    adj = _build_adj(n, edges)

    k_greedy = greedy_largest_first(n, adj)
    k_dsatur = dsatur(n, adj)
    print(f"{k_greedy} {k_dsatur}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

