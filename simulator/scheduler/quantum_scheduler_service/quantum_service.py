#!/usr/bin/env python3
"""
Quantum Scheduler Service - 极简健壮版
"""

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response
import json
import tempfile
import sys
import traceback
import random
import time
import os
from pathlib import Path
from datetime import datetime
import math

# 直接导入 Grover 算法
from grover_graph_coloring import GroverGraphColoring
from graph_analysis import GraphColoringAnalyzer

app = FastAPI()

# 时间记录日志文件
TIMING_LOG_FILE = Path("grover_timing.csv")
DESCENT_LOG_FILE = Path("grover_descent_log.csv")

def init_timing_log():
    """初始化时间记录文件"""
    if not TIMING_LOG_FILE.exists():
        with open(TIMING_LOG_FILE, 'w') as f:
            f.write("timestamp,endpoint,num_pods,num_edges,num_colors,grover_iterations,grover_time_ms,attempts,success\n")

def log_timing(endpoint, num_pods, num_edges, num_colors, grover_iterations, grover_time_sec, attempts, success):
    """记录Grover算法执行时间（毫秒精度）"""
    init_timing_log()
    grover_time_ms = grover_time_sec * 1000  # 转换为毫秒
    with open(TIMING_LOG_FILE, 'a') as f:
        f.write(f"{datetime.now().isoformat()},{endpoint},{num_pods},{num_edges},{num_colors},{grover_iterations},{grover_time_ms:.3f},{attempts},{success}\n")
    print(f"[TIMING] ⏱️ Grover求解时间: {grover_time_ms:.3f}ms (已记录到 {TIMING_LOG_FILE})", file=sys.stderr)


def init_descent_log():
    """初始化budget-descent日志文件"""
    if not DESCENT_LOG_FILE.exists():
        with open(DESCENT_LOG_FILE, "w") as f:
            f.write(
                "timestamp,endpoint,graph_name,num_pods,num_edges,num_nodes_budget,"
                "k_upper,k_start,k_found,attempt_budget,max_grover_iterations,bbht_lambda,"
                "attempts_total,oracle_calls_total,solve_time_ms,success,attempts_by_k,oracle_calls_by_k\n"
            )


def log_descent(
    *,
    endpoint: str,
    graph_name: str,
    num_pods: int,
    num_edges: int,
    num_nodes_budget: int,
    k_upper: int,
    k_start: int,
    k_found: int | None,
    attempt_budget: int,
    max_grover_iterations: int,
    bbht_lambda: float,
    attempts_total: int,
    oracle_calls_total: int,
    solve_time_ms: float,
    success: bool,
    attempts_by_k: dict,
    oracle_calls_by_k: dict,
):
    init_descent_log()
    with open(DESCENT_LOG_FILE, "a") as f:
        f.write(
            f"{datetime.now().isoformat()},{endpoint},{graph_name},{num_pods},{num_edges},{num_nodes_budget},"
            f"{k_upper},{k_start},{'' if k_found is None else int(k_found)},{attempt_budget},"
            f"{max_grover_iterations},{bbht_lambda},"
            f"{attempts_total},{oracle_calls_total},{solve_time_ms:.3f},{success},"
            f"\"{json.dumps(attempts_by_k, ensure_ascii=False)}\","
            f"\"{json.dumps(oracle_calls_by_k, ensure_ascii=False)}\"\n"
        )

@app.get("/health")
async def health():
    return {"status": "healthy"}

@app.post("/filter")
async def filter_nodes(request: Request):
    """Filter 始终通过"""
    try:
        body = await request.json()
        node_names = body.get("NodeNames", [])
        return Response(
            content=json.dumps({"NodeNames": node_names, "FailedNodes": {}, "Error": ""}),
            media_type="application/json"
        )
    except Exception as e:
        print(f"FILTER ERROR: {e}", file=sys.stderr)
        return Response(
            content=json.dumps({"NodeNames": [], "FailedNodes": {}, "Error": str(e)}),
            media_type="application/json"
        )

@app.post("/schedule")
async def schedule(request: Request):
    """Go QuantumScheduler plugin entrypoint.

    Expects JSON:
      {
        "pods": [
          {"pod_name": "region-0", "conflicts_with": ["region-1", ...]}
        ],
        "num_nodes": 5,
        "max_attempts": 20
      }

    Returns JSON matching ScheduleResponse:
      {
        "success": bool,
        "assignments": {"region-0": 1},  # node index (1-based)
        "attempts": int,
        "message": str,
        "grover_iterations": int,
        "error": str | null
      }
    """
    try:
        body = await request.json()
        pods = body.get("pods") or []
        num_nodes = int(body.get("num_nodes") or 0)
        max_attempts = int(body.get("max_attempts") or 8)

        if not pods or num_nodes <= 0:
            resp = {
                "success": False,
                "assignments": {},
                "attempts": 0,
                "message": "invalid request",
                "grover_iterations": 0,
                "error": "no pods or num_nodes <= 0",
            }
            return Response(content=json.dumps(resp), media_type="application/json")

        main = pods[0]
        pod_name = main.get("pod_name") or "unknown"
        conflicts = main.get("conflicts_with") or []

        print(f"\n[/schedule] pod={pod_name}, conflicts={conflicts}, num_nodes={num_nodes}", file=sys.stderr)

        # Build a simple star conflict graph: current pod connected to each conflict pod
        all_pods = [pod_name] + [c for c in conflicts if c != pod_name]
        # de-duplicate while preserving order
        all_pods = list(dict.fromkeys(all_pods))
        pod_to_idx = {name: i for i, name in enumerate(all_pods)}

        edges = []
        u = pod_to_idx[pod_name]
        for conflict in conflicts:
            if conflict in pod_to_idx:
                v = pod_to_idx[conflict]
                if u != v:
                    edge = sorted([u, v])
                    if edge not in edges:
                        edges.append(edge)

        graph_data = {
            "name": "k8s_runtime_graph_plugin",
            "nodes": len(all_pods),
            "edges": edges,
            "city_names": {str(i): name for i, name in enumerate(all_pods)},
        }

        print(f"[/schedule] graph: {len(all_pods)} nodes, {len(edges)} edges", file=sys.stderr)

        temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
        json.dump(graph_data, temp_file)
        temp_file.close()

        assignments = {}
        attempts = 0
        grover_iterations = 0

        try:
            # 先分析图推荐最优颜色数，然后与可用节点数取较小值
            temp_solver = GroverGraphColoring(temp_file.name, num_colors=None)
            recommended_colors = temp_solver.num_colors
            actual_colors_used = min(recommended_colors, num_nodes)
            if recommended_colors > num_nodes:
                actual_colors_used = recommended_colors
            
            print(f"[/schedule] 图分析: 推荐{recommended_colors}色, 可用{num_nodes}节点, 使用{actual_colors_used}色", file=sys.stderr)
            
            solver = GroverGraphColoring(temp_file.name, num_colors=actual_colors_used)
            grover_iterations = getattr(solver, "grover_iterations", 0)

            success, coloring, attempts, _, _, _ = solver.run_with_collapse_simulation(
                max_attempts=max_attempts,
                final_shots=0,
            )

            print(f"[/schedule] Grover finished: success={success}, attempts={attempts}", file=sys.stderr)

            if success and coloring:
                idx = pod_to_idx[pod_name]
                color = coloring.get(idx)
                if color is not None:
                    print(f"[/schedule] color for {pod_name}: {color}", file=sys.stderr)
                    # Map color (0-based) to node index (1-based) expected by Go plugin
                    node_idx = (int(color) % num_nodes) + 1
                    assignments[pod_name] = node_idx
                    print(f"[/schedule] assignment: {pod_name} -> node-{node_idx}", file=sys.stderr)
                    print(f"[/schedule] 📊 优化：使用{actual_colors_used}色 vs 可用{num_nodes}节点", file=sys.stderr)

        finally:
            Path(temp_file.name).unlink(missing_ok=True)

        resp = {
            "success": bool(assignments),
            "assignments": assignments,
            "attempts": int(attempts),
            "message": "ok" if assignments else "no assignment",
            "grover_iterations": int(grover_iterations),
        }
        if not assignments:
            resp["error"] = "no valid coloring or Grover failed"

        return Response(content=json.dumps(resp), media_type="application/json")

    except Exception as e:
        print(f"ERROR in /schedule: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        resp = {
            "success": False,
            "assignments": {},
            "attempts": 0,
            "message": "exception",
            "grover_iterations": 0,
            "error": str(e),
        }
        return Response(content=json.dumps(resp), media_type="application/json")

@app.post("/prioritize")
async def prioritize_nodes(request: Request):
    """Prioritize - 调用 Grover 算法"""
    try:
        # 1. 解析请求
        body = await request.json()
        pod = body.get("Pod", {})
        node_names = body.get("NodeNames") or []
        
        pod_name = pod.get("metadata", {}).get("name", "unknown")
        print(f"\n[{pod_name}] 收到调度请求, 可用节点: {node_names}", file=sys.stderr)
        
        if not node_names:
            return Response(content=json.dumps({"NodeList": [], "Error": ""}), media_type="application/json")

        # 2. 提取反亲和性冲突
        conflict_pods = []
        anti_affinity = pod.get("spec", {}).get("affinity", {}).get("podAntiAffinity", {})
        
        if anti_affinity:
            terms = anti_affinity.get("requiredDuringSchedulingIgnoredDuringExecution")
            if terms:
                for term in terms:
                    if "labelSelector" in term:
                        match_exprs = term["labelSelector"].get("matchExpressions")
                        if match_exprs:
                            for expr in match_exprs:
                                if expr.get("operator") == "In":
                                    values = expr.get("values")
                                    if values:
                                        conflict_pods.extend(values)
        
        print(f"[{pod_name}] 冲突列表: {conflict_pods}", file=sys.stderr)
        
        # 如果没有冲突，直接返回均衡分数
        if not conflict_pods:
            print(f"[{pod_name}] 无冲突，返回默认分数", file=sys.stderr)
            priorities = [{"Host": node, "Score": 50} for node in node_names]
            return Response(
                content=json.dumps({"NodeList": priorities, "Error": ""}),
                media_type="application/json"
            )

        # 3. 构建冲突图数据
        all_pods = [pod_name] + conflict_pods
        all_pods = list(set(all_pods))
        pod_to_idx = {name: i for i, name in enumerate(all_pods)}
        
        edges = []
        u = pod_to_idx[pod_name]
        for conflict in conflict_pods:
            if conflict in pod_to_idx:
                v = pod_to_idx[conflict]
                if u != v:
                    edge = sorted([u, v])
                    if edge not in edges:
                        edges.append(edge)
        
        graph_data = {
            "name": "k8s_runtime_graph",
            "nodes": len(all_pods),
            "edges": edges,
            "city_names": {str(i): name for i, name in enumerate(all_pods)}
        }
        
        print(f"[{pod_name}] 构建图完成: {len(all_pods)} 节点, {len(edges)} 边", file=sys.stderr)

        # 4. 运行 Grover 算法
        temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
        json.dump(graph_data, temp_file)
        temp_file.close()
        
        try:
            # 先用 None 让系统分析图结构并推荐最优颜色数
            temp_solver = GroverGraphColoring(temp_file.name, num_colors=None)
            recommended_colors = temp_solver.num_colors
            
            # 使用推荐颜色数与可用节点数中的较小值，确保有足够颜色且不浪费
            # 但至少要用推荐的颜色数，否则可能无解
            actual_colors_used = min(recommended_colors, len(node_names))
            # 如果推荐的比可用节点多，说明冲突密集，用推荐的
            if recommended_colors > len(node_names):
                actual_colors_used = recommended_colors
            
            print(f"[{pod_name}] 图分析: 推荐{recommended_colors}色, 可用{len(node_names)}节点, 使用{actual_colors_used}色", file=sys.stderr)
            
            solver = GroverGraphColoring(temp_file.name, num_colors=actual_colors_used)
            
            success, coloring, attempts, _, _, _ = solver.run_with_collapse_simulation(max_attempts=8, final_shots=0)
            
            print(f"[{pod_name}] Grover 运行结束. 成功={success}, 尝试={attempts}", file=sys.stderr)
            
            target_node = None
            
            if success and coloring:
                my_idx = pod_to_idx[pod_name]
                my_color = coloring.get(my_idx)
                
                if my_color is not None:
                    print(f"[{pod_name}] 量子计算出的颜色: {my_color}", file=sys.stderr)
                    
                    node_idx = my_color % len(node_names)
                    sorted_nodes = sorted(node_names)
                    target_node = sorted_nodes[node_idx]
                    
                    print(f"[{pod_name}] 🎯 量子推荐节点: {target_node} (颜色{my_color} → 节点索引{node_idx})", file=sys.stderr)
                    print(f"[{pod_name}] 📊 实际使用 {actual_colors_used} 种颜色，可最小化到 {actual_colors_used} 个节点", file=sys.stderr)
            
            # 5. 生成评分结果
            priorities = []
            for node in node_names:
                if target_node and node == target_node:
                    score = 100 # 强烈推荐
                elif target_node:
                    score = 0   # 其他节点不推荐
                else:
                    score = random.randint(0, 50) # 失败回退
                
                priorities.append({"Host": node, "Score": score})
                
            return Response(
                content=json.dumps({"NodeList": priorities, "Error": ""}),
                media_type="application/json"
            )

        finally:
            Path(temp_file.name).unlink(missing_ok=True)

    except Exception as e:
        print(f"ERROR in prioritize: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        return Response(
            content=json.dumps({"NodeList": [], "Error": str(e)}),
            media_type="application/json"
        )


@app.post("/batch_schedule")
async def batch_schedule(request: Request):
    """批量调度端点：一次性处理多个Pod的全局最优分配
    
    请求：
    {
        "pods": [
            {"pod_name": "region-0", "conflicts_with": ["region-1", ...]},
            {"pod_name": "region-1", "conflicts_with": [...]},
            ...
        ],
        "num_nodes": 5
    }
    
    响应：
    {
        "success": true,
        "assignments": {
            "region-0": "node-3",
            "region-1": "node-4",
            ...
        },
        "colors_used": 4
    }
    """
    try:
        body = await request.json()
        pods = body.get("pods", [])
        graph_name = str(body.get("graph_name") or body.get("name") or "k8s_batch_graph")
        num_nodes = int(body.get("num_nodes", 5))

        attempt_budget_raw = body.get("attempt_budget")
        if attempt_budget_raw is None:
            attempt_budget_raw = body.get("max_attempts")

        # 可选：用(p_min, delta)推导尝试次数T（不提供则使用默认或显式值）
        if attempt_budget_raw is None:
            p_min = body.get("p_min")
            delta = body.get("delta")
            if p_min is not None and delta is not None:
                p_min = float(p_min)
                delta = float(delta)
                if not (0.0 < p_min < 1.0):
                    raise ValueError("p_min must be in (0,1)")
                if not (0.0 < delta < 1.0):
                    raise ValueError("delta must be in (0,1)")
                attempt_budget_raw = int(math.ceil(math.log(delta) / math.log(1.0 - p_min)))
            else:
                attempt_budget_raw = int(
                    os.getenv("QUANTUM_DEFAULT_ATTEMPT_BUDGET")
                    or os.getenv("QUANTUM_ATTEMPT_BUDGET")
                    or "1"
                )

        attempt_budget = int(attempt_budget_raw)
        hard_max_attempt_budget = os.getenv("QUANTUM_HARD_MAX_ATTEMPT_BUDGET")
        if hard_max_attempt_budget is not None and hard_max_attempt_budget != "":
            try:
                hard_max_attempt_budget_int = int(hard_max_attempt_budget)
            except Exception:
                hard_max_attempt_budget_int = 0
            if hard_max_attempt_budget_int > 0:
                attempt_budget = min(attempt_budget, hard_max_attempt_budget_int)
        max_grover_iterations = int(
            body.get("max_grover_iterations")
            or body.get("max_iterations")
            or os.getenv("QUANTUM_MAX_GROVER_ITERATIONS")
            or "20"
        )
        hard_max_grover_iterations = os.getenv("QUANTUM_HARD_MAX_GROVER_ITERATIONS")
        if hard_max_grover_iterations is not None and hard_max_grover_iterations != "":
            try:
                hard_max_grover_iterations_int = int(hard_max_grover_iterations)
            except Exception:
                hard_max_grover_iterations_int = 0
            if hard_max_grover_iterations_int > 0:
                max_grover_iterations = min(max_grover_iterations, hard_max_grover_iterations_int)
        bbht_lambda = float(body.get("bbht_lambda") or 1.2)
        seed = body.get("seed")
        seed = int(seed) if seed is not None else None
        
        print(
            f"\n[/batch_schedule] 收到批量请求: {len(pods)} 个 Pod, {num_nodes} 个可用节点, "
            f"T={attempt_budget}, max_iter={max_grover_iterations}",
            file=sys.stderr,
        )
        
        if not pods:
            return {"success": False, "assignments": {}, "error": "no pods"}
        
        # 构建完整的全局冲突图（所有Pod）
        all_pod_names: list[str] = []
        for p in pods:
            pod_name = p.get("pod_name")
            if pod_name and pod_name not in all_pod_names:
                all_pod_names.append(pod_name)
            for conflict in p.get("conflicts_with", []) or []:
                if conflict and conflict not in all_pod_names:
                    all_pod_names.append(conflict)

        pod_to_idx = {name: i for i, name in enumerate(all_pod_names)}

        edges: list[list[int]] = []
        edges_seen: set[tuple[int, int]] = set()
        for pod in pods:
            pod_name = pod["pod_name"]
            idx1 = pod_to_idx[pod_name]
            for conflict in pod.get("conflicts_with", []):
                if conflict in pod_to_idx:
                    idx2 = pod_to_idx[conflict]
                    u, v = (idx1, idx2) if idx1 <= idx2 else (idx2, idx1)
                    if u == v:
                        continue
                    if (u, v) in edges_seen:
                        continue
                    edges_seen.add((u, v))
                    edges.append([u, v])
        
        graph_data = {
            "name": graph_name,
            "nodes": len(all_pod_names),
            "edges": edges
        }
        
        print(f"[/batch_schedule] 构建全局图: {len(all_pod_names)} 节点, {len(edges)} 条边", file=sys.stderr)
        
        # 写临时文件
        temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
        json.dump(graph_data, temp_file)
        temp_file.close()
        
        try:
            analyzer = GraphColoringAnalyzer(temp_file.name)
            k_u = int(analyzer.analysis.get("recommended_colors") or 0)
            k_start = min(num_nodes, k_u) if num_nodes > 0 else k_u

            if len(edges) == 0:
                if num_nodes <= 0:
                    return {"success": False, "assignments": {}, "error": "num_nodes <= 0"}

                assignments = {pod_name: "node-1" for pod_name in all_pod_names}
                return {
                    "success": True,
                    "assignments": assignments,
                    "colors_used": 1,
                    "attempts": 0,
                    "k_upper": k_u,
                    "k_found": 1,
                    "message": "edgeless graph",
                }

            if k_start < 2:
                return {
                    "success": False,
                    "assignments": {},
                    "error": "insufficient node budget",
                    "k_upper": k_u,
                    "k_start": k_start,
                }

            print(
                f"[/batch_schedule] 图分析: greedy上界 k_u={k_u}, 节点预算 k_start={k_start}",
                file=sys.stderr,
            )

            # budget-descent: k_start, k_start-1, ... ; stop after first failure
            best_k = None
            best_coloring = None
            attempts_total = 0
            solve_time_ms_total = 0.0
            attempts_by_k = {}
            oracle_calls_total = 0
            oracle_calls_by_k = {}

            for k in range(k_start, 1, -1):
                solver = GroverGraphColoring(temp_file.name, num_colors=k)

                success, coloring, attempts, _, _, solve_time_ms = solver.run_with_bbht_collapse_simulation(
                    max_attempts=attempt_budget,
                    final_shots=0,
                    max_grover_iterations=max_grover_iterations,
                    lambda_factor=bbht_lambda,
                    seed=(None if seed is None else seed + (k * 1000)),
                    verbose=False,
                )

                attempts_total += int(attempts)
                solve_time_ms_total += float(solve_time_ms)
                attempts_by_k[str(k)] = int(attempts)
                oracle_calls = int(getattr(solver, "last_bbht_stats", {}).get("oracle_calls") or 0)
                oracle_calls_total += oracle_calls
                oracle_calls_by_k[str(k)] = oracle_calls

                if success and coloring:
                    best_k = k
                    best_coloring = coloring
                    print(
                        f"[/batch_schedule] ✓ k={k} 可行 (attempts={attempts}/{attempt_budget}, "
                        f"max_iter={max_grover_iterations})",
                        file=sys.stderr,
                    )
                    continue

                print(
                    f"[/batch_schedule] ✗ k={k} 未找到解 (attempts={attempts}/{attempt_budget}, "
                    f"max_iter={max_grover_iterations})，停止下降",
                    file=sys.stderr,
                )
                break

            success = best_coloring is not None and best_k is not None

            # 记录到日志文件（时间已经是毫秒）
            log_timing(
                "/batch_schedule",
                len(all_pod_names),
                len(edges),
                int(best_k or k_start),
                max_grover_iterations,
                solve_time_ms_total / 1000,
                attempts_total,
                success,
            )

            log_descent(
                endpoint="/batch_schedule",
                graph_name=graph_name,
                num_pods=len(all_pod_names),
                num_edges=len(edges),
                num_nodes_budget=num_nodes,
                k_upper=k_u,
                k_start=k_start,
                k_found=best_k,
                attempt_budget=attempt_budget,
                max_grover_iterations=max_grover_iterations,
                bbht_lambda=bbht_lambda,
                attempts_total=attempts_total,
                oracle_calls_total=oracle_calls_total,
                solve_time_ms=solve_time_ms_total,
                success=success,
                attempts_by_k=attempts_by_k,
                oracle_calls_by_k=oracle_calls_by_k,
            )

            print(
                f"[/batch_schedule] Descent完成: success={success}, k_found={best_k}, "
                f"attempts_total={attempts_total}, 求解时间={solve_time_ms_total:.3f}ms",
                file=sys.stderr,
            )
            
            # 映射颜色到节点名
            assignments = {}
            if success and best_coloring:
                if best_k > num_nodes:
                    return {
                        "success": False,
                        "assignments": {},
                        "error": "k_found exceeds available nodes",
                        "k_upper": k_u,
                        "k_found": best_k,
                        "num_nodes": num_nodes,
                    }

                for pod_name in all_pod_names:
                    idx = pod_to_idx[pod_name]
                    color = best_coloring.get(idx)
                    if color is not None:
                        node_idx = int(color) + 1
                        node_name = f"node-{node_idx}"
                        assignments[pod_name] = node_name
                        print(f"[/batch_schedule] ✓ {pod_name} → {node_name} (颜色{color})", file=sys.stderr)
                
                print(f"[/batch_schedule] 📊 成功分配 {len(assignments)} 个Pod，使用{best_k}色", file=sys.stderr)
            
            return {
                "success": bool(assignments),
                "assignments": assignments,
                "colors_used": int(best_k or 0),
                "attempts": int(attempts_total),
                "oracle_calls": int(oracle_calls_total),
                "k_upper": k_u,
                "k_start": k_start,
                "k_found": best_k,
                "attempts_by_k": attempts_by_k,
                "oracle_calls_by_k": oracle_calls_by_k,
                "attempt_budget": attempt_budget,
                "max_grover_iterations": max_grover_iterations,
                "bbht_lambda": bbht_lambda,
                "seed": seed,
            }
        
        finally:
            Path(temp_file.name).unlink(missing_ok=True)
    
    except Exception as e:
        print(f"[/batch_schedule] ERROR: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        return {"success": False, "assignments": {}, "error": str(e)}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")
