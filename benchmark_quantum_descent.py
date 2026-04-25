#!/usr/bin/env python3
"""
Benchmark Grover budget-descent (/batch_schedule) on a directory of JSON graphs.

This script is designed for Chapter 5 data collection:
- solution quality: k_greedy (upper bound) vs k_opt (exact) vs k_found (quantum descent)
- speed proxy: total oracle calls (sum of Grover iterations across BBHT attempts)

Default input graphs live in ../input (repo root).
"""

from __future__ import annotations

import argparse
import csv
import json
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


def _build_adj(n: int, edges: list[list[int]]) -> list[set[int]]:
    adj: list[set[int]] = [set() for _ in range(n)]
    for u, v in edges:
        if u == v:
            continue
        adj[u].add(v)
        adj[v].add(u)
    return adj


def _greedy_largest_first(n: int, adj: list[set[int]]) -> int:
    order = sorted(range(n), key=lambda i: len(adj[i]), reverse=True)
    color: list[int | None] = [None] * n
    for u in order:
        used = {color[v] for v in adj[u] if color[v] is not None}
        c = 0
        while c in used:
            c += 1
        color[u] = c
    return (max(color) + 1) if color and any(c is not None for c in color) else 0


def _greedy_dsatur(n: int, adj: list[set[int]]) -> int:
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

    return (max(color) + 1) if color and any(c is not None for c in color) else 0


def _chromatic_number_exact(n: int, adj: list[set[int]]) -> int:
    order = sorted(range(n), key=lambda i: len(adj[i]), reverse=True)
    best = n
    color = [-1] * n

    def can_use(v: int, c: int) -> bool:
        return all(color[u] != c for u in adj[v])

    def dfs(pos: int, used: int) -> None:
        nonlocal best
        if used >= best:
            return
        if pos == n:
            best = min(best, used)
            return
        v = order[pos]
        for c in range(used):
            if can_use(v, c):
                color[v] = c
                dfs(pos + 1, used)
                color[v] = -1
        if used + 1 < best:
            color[v] = used
            dfs(pos + 1, used + 1)
            color[v] = -1

    dfs(0, 0)
    return best


def _post_json(url: str, payload: dict) -> dict:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=3600) as resp:
        body = resp.read().decode("utf-8")
    return json.loads(body)


@dataclass(frozen=True)
class GraphCase:
    name: str
    n: int
    m: int
    adj: list[set[int]]


def _load_graph(json_path: Path) -> GraphCase:
    data = json.loads(json_path.read_text(encoding="utf-8"))
    name = str(data.get("name") or json_path.stem)
    n = int(data["nodes"])
    edges = data.get("edges") or []
    adj = _build_adj(n, edges)
    return GraphCase(name=name, n=n, m=len(edges), adj=adj)


def _build_pods_payload(case: GraphCase) -> list[dict]:
    pods: list[dict] = []
    for i in range(case.n):
        pod_name = f"pod-{i}"
        conflicts = [f"pod-{j}" for j in sorted(case.adj[i])]
        pods.append({"pod_name": pod_name, "conflicts_with": conflicts})
    return pods


def main() -> int:
    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parent
    default_input_dir = repo_root / "input"
    default_out = script_dir / "results" / "quantum_descent_benchmark.csv"

    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", type=Path, default=default_input_dir)
    parser.add_argument("--service-url", type=str, default="http://localhost:8000/batch_schedule")
    parser.add_argument("--out", type=Path, default=default_out)
    parser.add_argument("--num-nodes", type=int, default=5)
    parser.add_argument("--attempt-budget", type=int, default=10)
    parser.add_argument("--max-grover-iterations", type=int, default=20)
    parser.add_argument("--bbht-lambda", type=float, default=1.2)
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()

    args.out.parent.mkdir(parents=True, exist_ok=True)

    json_files = sorted(args.input_dir.glob("*.json"))
    if not json_files:
        raise SystemExit(f"No JSON graphs found in: {args.input_dir}")

    fieldnames = [
        "timestamp",
        "graph",
        "n",
        "m",
        "node_budget",
        "k_greedy",
        "k_dsatur",
        "k_opt",
        "k_upper",
        "k_start",
        "k_found",
        "success",
        "attempts_total",
        "oracle_calls_total",
        "attempt_budget",
        "max_grover_iterations",
        "bbht_lambda",
        "seed",
        "wall_time_ms",
        "attempts_by_k",
        "oracle_calls_by_k",
        "error",
    ]

    write_header = not args.out.exists()
    with args.out.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()

        for json_path in json_files:
            case = _load_graph(json_path)
            k_greedy = _greedy_largest_first(case.n, case.adj)
            k_dsatur = _greedy_dsatur(case.n, case.adj)
            k_opt = _chromatic_number_exact(case.n, case.adj)

            payload = {
                "graph_name": case.name,
                "pods": _build_pods_payload(case),
                "num_nodes": args.num_nodes,
                "attempt_budget": args.attempt_budget,
                "max_grover_iterations": args.max_grover_iterations,
                "bbht_lambda": args.bbht_lambda,
                "seed": args.seed,
            }

            wall_start = time.time()
            error = ""
            response: dict = {}
            try:
                response = _post_json(args.service_url, payload)
            except urllib.error.URLError as e:
                error = f"URLError: {e}"
            except Exception as e:
                error = f"Exception: {e}"
            wall_ms = (time.time() - wall_start) * 1000

            writer.writerow(
                {
                    "timestamp": datetime.now().isoformat(),
                    "graph": case.name,
                    "n": case.n,
                    "m": case.m,
                    "node_budget": args.num_nodes,
                    "k_greedy": k_greedy,
                    "k_dsatur": k_dsatur,
                    "k_opt": k_opt,
                    "k_upper": response.get("k_upper"),
                    "k_start": response.get("k_start"),
                    "k_found": response.get("k_found"),
                    "success": bool(response.get("success")),
                    "attempts_total": response.get("attempts"),
                    "oracle_calls_total": response.get("oracle_calls"),
                    "attempt_budget": response.get("attempt_budget"),
                    "max_grover_iterations": response.get("max_grover_iterations"),
                    "bbht_lambda": response.get("bbht_lambda"),
                    "seed": response.get("seed"),
                    "wall_time_ms": f"{wall_ms:.3f}",
                    "attempts_by_k": json.dumps(response.get("attempts_by_k", {}), ensure_ascii=False),
                    "oracle_calls_by_k": json.dumps(response.get("oracle_calls_by_k", {}), ensure_ascii=False),
                    "error": error or response.get("error", ""),
                }
            )

            print(
                f"[{case.name}] n={case.n} m={case.m} "
                f"k_opt={k_opt} greedy={k_greedy} dsatur={k_dsatur} "
                f"-> k_found={response.get('k_found')} "
                f"oracle_calls={response.get('oracle_calls')} "
                f"success={response.get('success')} "
                f"time={wall_ms:.1f}ms"
            )

    print(f"\nSaved: {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
