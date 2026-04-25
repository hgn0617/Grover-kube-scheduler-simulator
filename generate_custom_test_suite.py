#!/usr/bin/env python3
"""
Generate a curated + reproducible test-suite of conflict graphs that expose
failures of classic coloring heuristics (Greedy Largest-First / DSATUR),
while staying within an estimated statevector-memory budget for our simulator.

Outputs:
- JSON graphs into ../input/ (repo root)
- Gated Kubernetes YAML into ./TEST_INPUT/

Why this exists:
- Current oracle uses ancillas proportional to |V|+|E|+1, so memory grows as 2^(n*qpn+n+m+1).
- We therefore need carefully sized graphs to keep simulation practical (target: <=128GiB for the custom suite).

NOTE (2026-04-21): The 60-case numbering here is the ORIGINAL pre-renumber
layout. After the 50/10 split (see scripts/renumber_50_cases.py) the on-disk
files have been renumbered: kept cases 01-50, deleted cases 51-60. If this
generator is re-run, it will create orphan files with the OLD numbering (e.g.
35_greedy_fail_bipartite_n8_m8.json) that no longer match config/*.txt or the
benchmark pipeline. DO NOT re-run without first updating the case.name fields
to the post-renumber stems. See scripts/renumber_50_cases.py::MAPPING.
"""

from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


def _build_adj(n: int, edges: list[tuple[int, int]]) -> list[set[int]]:
    adj: list[set[int]] = [set() for _ in range(n)]
    for u, v in edges:
        if u == v:
            continue
        adj[u].add(v)
        adj[v].add(u)
    return adj


def greedy_largest_first(n: int, adj: list[set[int]]) -> int:
    order = sorted(range(n), key=lambda i: len(adj[i]), reverse=True)
    color: list[int | None] = [None] * n
    for u in order:
        used = {color[v] for v in adj[u] if color[v] is not None}
        c = 0
        while c in used:
            c += 1
        color[u] = c
    return (max(color) + 1) if n > 0 else 0


def dsatur(n: int, adj: list[set[int]]) -> int:
    if n == 0:
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

    return max(color) + 1


def chromatic_number_exact(n: int, adj: list[set[int]]) -> int:
    if n == 0:
        return 0
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


def estimate_total_qubits(n: int, m: int, k_start: int) -> int:
    if k_start <= 1:
        return 0
    qubits_per_node = int(math.ceil(math.log2(k_start)))
    main = n * qubits_per_node
    anc = n + m + 1
    return main + anc


def estimate_memory_gib(total_qubits: int) -> float:
    if total_qubits <= 0:
        return 0.0
    return (2**total_qubits * 16) / (1024**3)


@dataclass(frozen=True)
class Case:
    name: str
    n: int
    edges: list[tuple[int, int]]


def _preset_cases() -> list[Case]:
    return [
        # Both Greedy(LF) and DSATUR return 4 while χ(G)=3 (n=7, m=10).
        Case(
            name="31_both_fail_n7_m11",
            n=7,
            edges=[
                (0, 3),
                (0, 5),
                (0, 6),
                (1, 2),
                (1, 4),
                (1, 6),
                (2, 6),
                (3, 4),
                (3, 5),
                (4, 5),
            ],
        ),
        # Greedy(LF)=4 while χ(G)=3; DSATUR finds 3 (n=6, m=9).
        Case(
            name="32_greedy_fail_dense_n7_m12",
            n=6,
            edges=[
                (0, 1),
                (0, 2),
                (0, 5),
                (1, 3),
                (2, 4),
                (2, 5),
                (3, 4),
                (3, 5),
                (4, 5),
            ],
        ),
        Case(
            name="33_greedy_fail_n8_m9_a",
            n=6,
            edges=[
                (0, 1),
                (0, 2),
                (1, 3),
                (1, 5),
                (2, 4),
                (2, 5),
                (3, 4),
                (3, 5),
                (4, 5),
            ],
        ),
        Case(
            name="34_greedy_fail_n8_m9_b",
            n=6,
            edges=[
                (0, 1),
                (0, 2),
                (0, 5),
                (1, 3),
                (1, 4),
                (1, 5),
                (2, 3),
                (3, 4),
                (4, 5),
            ],
        ),
        # Bipartite: χ(G)=2 but Greedy(LF)=3; DSATUR returns 2.
        Case(
            name="35_greedy_fail_bipartite_n8_m8",
            n=8,
            edges=[
                (6, 7),
                (2, 3),
                (0, 5),
                (0, 6),
                (3, 5),
                (0, 4),
                (2, 7),
                (1, 5),
            ],
        ),
        Case(
            name="36_greedy_fail_n7_m12_b",
            n=6,
            edges=[
                (0, 1),
                (0, 2),
                (1, 4),
                (1, 5),
                (2, 3),
                (2, 5),
                (3, 4),
                (3, 5),
                (4, 5),
            ],
        ),
        Case(
            name="37_greedy_fail_n7_m12_c",
            n=6,
            edges=[
                (0, 2),
                (0, 4),
                (0, 5),
                (1, 2),
                (1, 3),
                (1, 4),
                (2, 3),
                (3, 5),
                (4, 5),
            ],
        ),
        Case(
            name="38_greedy_fail_n7_m12_d",
            n=6,
            edges=[
                (0, 2),
                (0, 4),
                (0, 5),
                (1, 2),
                (1, 3),
                (1, 4),
                (2, 3),
                (2, 5),
                (3, 5),
            ],
        ),
        Case(
            name="39_greedy_fail_n7_m12_e",
            n=6,
            edges=[
                (0, 1),
                (0, 2),
                (0, 4),
                (0, 5),
                (1, 3),
                (1, 4),
                (2, 3),
                (2, 5),
                (4, 5),
            ],
        ),
        Case(
            name="40_greedy_fail_n8_m9_c",
            n=6,
            edges=[
                (0, 2),
                (0, 3),
                (0, 4),
                (1, 2),
                (1, 4),
                (1, 5),
                (2, 3),
                (2, 5),
                (3, 5),
            ],
        ),
        Case(
            name="41_greedy_fail_n8_m9_d",
            n=6,
            edges=[
                (0, 3),
                (0, 4),
                (0, 5),
                (1, 2),
                (1, 3),
                (1, 5),
                (2, 3),
                (2, 4),
                (3, 4),
            ],
        ),
        Case(
            name="42_greedy_fail_n8_m9_e",
            n=6,
            edges=[
                (0, 1),
                (0, 2),
                (1, 3),
                (1, 4),
                (2, 3),
                (2, 5),
                (3, 4),
                (3, 5),
                (4, 5),
            ],
        ),
        Case(
            name="43_greedy_fail_n8_m9_f",
            n=6,
            edges=[
                (0, 1),
                (0, 3),
                (0, 5),
                (1, 2),
                (1, 3),
                (1, 4),
                (2, 4),
                (2, 5),
                (3, 4),
            ],
        ),
        # Bipartite: χ(G)=2 but Greedy(LF)=3; DSATUR=2.
        Case(
            name="44_greedy_fail_bipartite_n8_m7_a",
            n=8,
            edges=[
                (5, 6),
                (4, 7),
                (1, 7),
                (1, 5),
                (0, 6),
                (3, 4),
                (2, 3),
            ],
        ),
        Case(
            name="45_greedy_fail_bipartite_n8_m8_b",
            n=8,
            edges=[
                (0, 1),
                (4, 6),
                (4, 5),
                (0, 4),
                (6, 7),
                (1, 2),
                (2, 3),
                (2, 7),
            ],
        ),
        # Both Greedy(LF) and DSATUR return 4 while χ(G)=3 (n=7, m=10).
        Case(
            name="46_both_fail_n7_m11_b",
            n=7,
            edges=[
                (0, 2),
                (0, 4),
                (0, 5),
                (1, 3),
                (1, 5),
                (1, 6),
                (2, 3),
                (2, 4),
                (3, 4),
                (5, 6),
            ],
        ),
        # Both Greedy(LF) and DSATUR return 4 while χ(G)=3 (n=7, m=10).
        Case(
            name="47_both_fail_n7_m11_c",
            n=7,
            edges=[
                (0, 1),
                (0, 2),
                (0, 6),
                (1, 2),
                (1, 5),
                (2, 5),
                (3, 4),
                (3, 6),
                (4, 5),
                (4, 6),
            ],
        ),
        Case(
            name="48_greedy_fail_n7_m12_f",
            n=6,
            edges=[
                (0, 2),
                (0, 4),
                (0, 5),
                (1, 2),
                (1, 3),
                (1, 5),
                (3, 4),
                (3, 5),
                (4, 5),
            ],
        ),
        Case(
            name="49_greedy_fail_n7_m12_g",
            n=6,
            edges=[
                (0, 2),
                (0, 3),
                (0, 4),
                (1, 2),
                (1, 3),
                (1, 5),
                (2, 5),
                (3, 4),
                (4, 5),
            ],
        ),
        Case(
            name="50_greedy_fail_n7_m12_h",
            n=6,
            edges=[
                (0, 1),
                (0, 2),
                (0, 3),
                (0, 4),
                (1, 3),
                (1, 5),
                (2, 4),
                (2, 5),
                (3, 4),
            ],
        ),
        Case(
            name="51_greedy_fail_n7_m12_i",
            n=6,
            edges=[
                (0, 1),
                (0, 2),
                (0, 5),
                (1, 2),
                (1, 4),
                (2, 3),
                (2, 4),
                (3, 4),
                (3, 5),
            ],
        ),
        Case(
            name="52_greedy_fail_n8_m9_g",
            n=6,
            edges=[
                (0, 1),
                (0, 3),
                (0, 5),
                (1, 4),
                (1, 5),
                (2, 3),
                (2, 4),
                (2, 5),
                (4, 5),
            ],
        ),
        Case(
            name="53_greedy_fail_n8_m9_h",
            n=6,
            edges=[
                (0, 1),
                (0, 3),
                (1, 2),
                (1, 5),
                (2, 3),
                (2, 4),
                (2, 5),
                (3, 4),
                (4, 5),
            ],
        ),
        # Bipartite: χ(G)=2 but Greedy(LF)=3; DSATUR=2.
        Case(
            name="54_greedy_fail_bipartite_n8_m7_b",
            n=8,
            edges=[
                (0, 2),
                (0, 4),
                (1, 3),
                (1, 6),
                (3, 7),
                (4, 6),
                (5, 7),
            ],
        ),
        Case(
            name="55_greedy_fail_bipartite_n8_m8_c",
            n=8,
            edges=[
                (0, 3),
                (1, 2),
                (1, 6),
                (2, 7),
                (3, 5),
                (4, 6),
                (5, 7),
                (6, 7),
            ],
        ),
        # Additional DSATUR failures (and Greedy failures): DSATUR=4 while χ(G)=3 (n=7, m=10).
        Case(
            name="56_dsatur_fail_n7_m10_a",
            n=7,
            edges=[
                (0, 1),
                (0, 2),
                (0, 3),
                (1, 2),
                (1, 5),
                (2, 5),
                (3, 4),
                (3, 6),
                (4, 5),
                (4, 6),
            ],
        ),
        Case(
            name="57_dsatur_fail_n7_m10_b",
            n=7,
            edges=[
                (0, 1),
                (0, 2),
                (0, 3),
                (1, 2),
                (1, 6),
                (2, 6),
                (3, 4),
                (3, 5),
                (4, 5),
                (4, 6),
            ],
        ),
        Case(
            name="58_dsatur_fail_n7_m10_c",
            n=7,
            edges=[
                (0, 1),
                (0, 2),
                (0, 3),
                (1, 2),
                (1, 6),
                (2, 6),
                (3, 4),
                (3, 5),
                (4, 5),
                (5, 6),
            ],
        ),
        Case(
            name="59_dsatur_fail_n7_m10_d",
            n=7,
            edges=[
                (0, 1),
                (0, 2),
                (0, 4),
                (1, 2),
                (1, 5),
                (2, 5),
                (3, 4),
                (3, 5),
                (3, 6),
                (4, 6),
            ],
        ),
        Case(
            name="60_dsatur_fail_n7_m10_e",
            n=7,
            edges=[
                (0, 1),
                (0, 2),
                (0, 4),
                (1, 2),
                (1, 6),
                (2, 6),
                (3, 4),
                (3, 5),
                (3, 6),
                (4, 5),
            ],
        ),
    ]


def _normalize_edges(edges: list[tuple[int, int]]) -> list[list[int]]:
    seen: set[tuple[int, int]] = set()
    out: list[list[int]] = []
    for u, v in edges:
        a, b = (u, v) if u <= v else (v, u)
        if a == b:
            continue
        if (a, b) in seen:
            continue
        seen.add((a, b))
        out.append([a, b])
    out.sort()
    return out


def _write_json_graph(out_path: Path, case: Case, *, memory_limit_gib: float) -> dict:
    edges = _normalize_edges(case.edges)
    n = case.n
    m = len(edges)
    adj = _build_adj(n, [(u, v) for u, v in edges])

    k_opt = chromatic_number_exact(n, adj)
    k_g = greedy_largest_first(n, adj)
    k_d = dsatur(n, adj)

    total_qubits = estimate_total_qubits(n, m, k_g)
    mem_gib = estimate_memory_gib(total_qubits)

    if mem_gib > memory_limit_gib:
        raise ValueError(
            f"{case.name}: estimated memory {mem_gib:.1f}GiB exceeds limit {memory_limit_gib:.1f}GiB "
            f"(n={n}, m={m}, k_g={k_g}, qubits={total_qubits})"
        )

    graph = {
        "name": case.name,
        "description": (
            f"Hard instance for evaluation: chi={k_opt}, greedy(LF)={k_g}, DSATUR={k_d}; "
            f"estimated_qubits={total_qubits}, estimated_mem_gib={mem_gib:.1f}."
        ),
        "nodes": n,
        "edges": edges,
        "city_names": {str(i): f"pod-{i}" for i in range(n)},
        "meta": {
            "k_opt": k_opt,
            "k_greedy_lf": k_g,
            "k_dsatur": k_d,
            "estimated_total_qubits_at_k_upper": total_qubits,
            "estimated_statevector_mem_gib_at_k_upper": round(mem_gib, 3),
            "generated_at": datetime.now().isoformat(),
        },
    }

    out_path.write_text(json.dumps(graph, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return graph


def _write_k8s_yaml(
    graph: dict,
    *,
    out_path: Path,
    include_gates: bool,
    include_quantum_labels: bool,
) -> None:
    name = graph["name"]
    n = int(graph["nodes"])
    edges = [tuple(e) for e in graph.get("edges", [])]

    adjacency: dict[int, list[int]] = {i: [] for i in range(n)}
    for u, v in edges:
        adjacency[u].append(v)
        adjacency[v].append(u)

    for i in range(n):
        adjacency[i] = sorted(set(adjacency[i]))

    # Nodes (at least n, and at least 5 to match existing tests)
    num_k8s_nodes = max(n, 5)

    lines: list[str] = []
    lines.append(f"# {graph.get('description', '')}")
    if "meta" in graph:
        meta = graph["meta"]
        lines.append(
            f"# meta: k_opt={meta.get('k_opt')}, k_greedy={meta.get('k_greedy_lf')}, "
            f"k_dsatur={meta.get('k_dsatur')}, qubits={meta.get('estimated_total_qubits_at_k_upper')}, "
            f"mem_gib≈{meta.get('estimated_statevector_mem_gib_at_k_upper')}"
        )
    lines.append(f"# {n} pods, {len(edges)} edges, {num_k8s_nodes} nodes in cluster")
    lines.append("")

    for i in range(1, num_k8s_nodes + 1):
        lines.extend(
            [
                "---",
                "apiVersion: v1",
                "kind: Node",
                "metadata:",
                f"  name: node-{i}",
                "  labels:",
                f"    kubernetes.io/hostname: node-{i}",
                "    node-role.kubernetes.io/worker: \"\"",
                "status:",
                "  capacity:",
                '    cpu: \"8\"',
                "    memory: 16Gi",
                '    pods: \"110\"',
                "  allocatable:",
                '    cpu: \"8\"',
                "    memory: 16Gi",
                '    pods: \"110\"',
            ]
        )

    for pod_idx in range(n):
        pod_name = f"pod-{pod_idx}"
        app_label = f"app-{pod_idx}"
        neighbors = adjacency[pod_idx]
        neighbor_labels = [f"app-{j}" for j in neighbors]

        lines.extend(
            [
                "---",
                f"# Pod {pod_idx} (conflicts_with: {', '.join(f'pod-{j}' for j in neighbors)})",
                "apiVersion: v1",
                "kind: Pod",
                "metadata:",
                f"  name: {pod_name}",
                "  labels:",
                f"    app: {app_label}",
            ]
        )

        if include_quantum_labels:
            lines.extend(
                [
                    f"    quantum-batch: \"{name}\"",
                    "  annotations:",
                    f"    quantum-scheduler.io/batch-size: \"{n}\"",
                ]
            )

        lines.extend(
            [
                "spec:",
            ]
        )

        if include_gates:
            lines.extend(
                [
                    "  schedulingGates:",
                    "  - name: \"quantum-scheduler.io/computing\"",
                ]
            )

        lines.extend(
            [
                "  containers:",
                "  - name: app",
                "    image: nginx",
                "    resources:",
                "      requests:",
                '        cpu: \"1\"',
                "        memory: 1Gi",
            ]
        )

        if neighbor_labels:
            lines.extend(
                [
                    "  affinity:",
                    "    podAntiAffinity:",
                    "      requiredDuringSchedulingIgnoredDuringExecution:",
                    "      - labelSelector:",
                    "          matchExpressions:",
                    "          - key: app",
                    "            operator: In",
                    f"            values: {json.dumps(neighbor_labels)}",
                    "        topologyKey: kubernetes.io/hostname",
                ]
            )

    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    here = Path(__file__).resolve().parent
    repo_root = here.parent
    input_dir = repo_root / "input"

    parser = argparse.ArgumentParser()
    parser.add_argument("--memory-limit-gib", type=float, default=128.0)
    args = parser.parse_args()

    test_input_dir = here / "TEST_INPUT"
    test_input_dir.mkdir(parents=True, exist_ok=True)

    input_dir.mkdir(parents=True, exist_ok=True)

    summary_rows: list[dict] = []

    for case in _preset_cases():
        json_path = input_dir / f"{case.name}.json"
        expected_edges = _normalize_edges(case.edges)

        graph: dict | None = None
        if json_path.exists():
            try:
                graph = json.loads(json_path.read_text(encoding="utf-8"))
            except Exception:
                graph = None

        if (
            graph is None
            or graph.get("name") != case.name
            or int(graph.get("nodes", -1)) != int(case.n)
            or _normalize_edges([tuple(e) for e in graph.get("edges", [])]) != expected_edges
        ):
            graph = _write_json_graph(json_path, case, memory_limit_gib=float(args.memory_limit_gib))

        # gated yaml (quantum)
        gated_yaml = test_input_dir / f"{case.name}.yaml"
        if not gated_yaml.exists():
            _write_k8s_yaml(graph, out_path=gated_yaml, include_gates=True, include_quantum_labels=True)

        meta = graph.get("meta", {})
        summary_rows.append(
            {
                "case": case.name,
                "n": graph["nodes"],
                "m": len(graph.get("edges", [])),
                "k_opt": meta.get("k_opt"),
                "k_greedy": meta.get("k_greedy_lf"),
                "k_dsatur": meta.get("k_dsatur"),
                "qubits": meta.get("estimated_total_qubits_at_k_upper"),
                "mem_gib": meta.get("estimated_statevector_mem_gib_at_k_upper"),
            }
        )

    summary_path = here / "results" / "custom_test_suite_summary.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary_rows, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    print(f"Wrote {len(summary_rows)} cases.")
    print(f"- JSON: {input_dir}")
    print(f"- YAML (gated): {test_input_dir}")
    print(f"- Summary: {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
