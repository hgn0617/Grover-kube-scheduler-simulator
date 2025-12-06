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
from pathlib import Path

# 直接导入 Grover 算法
from grover_graph_coloring import GroverGraphColoring

app = FastAPI()

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
        max_attempts = int(body.get("max_attempts") or 20)

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

            success, coloring, attempts, _, _ = solver.run_with_collapse_simulation(
                max_attempts=max_attempts,
                final_shots=100,
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
            
            success, coloring, attempts, _, _ = solver.run_with_collapse_simulation(max_attempts=20, final_shots=100)
            
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
        num_nodes = body.get("num_nodes", 5)
        
        print(f"\n[/batch_schedule] 收到批量请求: {len(pods)} 个 Pod, {num_nodes} 个可用节点", file=sys.stderr)
        
        if not pods:
            return {"success": False, "assignments": {}, "error": "no pods"}
        
        # 构建完整的全局冲突图（所有Pod）
        all_pod_names = [p["pod_name"] for p in pods]
        pod_to_idx = {name: i for i, name in enumerate(all_pod_names)}
        
        edges = []
        for pod in pods:
            pod_name = pod["pod_name"]
            idx1 = pod_to_idx[pod_name]
            for conflict in pod.get("conflicts_with", []):
                if conflict in pod_to_idx:
                    idx2 = pod_to_idx[conflict]
                    edge = sorted([idx1, idx2])
                    if edge not in edges:
                        edges.append(edge)
        
        graph_data = {
            "name": "k8s_batch_graph",
            "nodes": len(all_pod_names),
            "edges": edges
        }
        
        print(f"[/batch_schedule] 构建全局图: {len(all_pod_names)} 节点, {len(edges)} 条边", file=sys.stderr)
        
        # 写临时文件
        temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
        json.dump(graph_data, temp_file)
        temp_file.close()
        
        try:
            # 图分析 + Grover求解
            temp_solver = GroverGraphColoring(temp_file.name, num_colors=None)
            recommended_colors = temp_solver.num_colors
            actual_colors = min(recommended_colors, num_nodes)
            if recommended_colors > num_nodes:
                actual_colors = recommended_colors
            
            print(f"[/batch_schedule] 图分析: 推荐{recommended_colors}色, 可用{num_nodes}节点, 使用{actual_colors}色", file=sys.stderr)
            
            solver = GroverGraphColoring(temp_file.name, num_colors=actual_colors)
            success, coloring, attempts, _, _ = solver.run_with_collapse_simulation(max_attempts=20, final_shots=100)
            
            print(f"[/batch_schedule] Grover完成: success={success}, attempts={attempts}", file=sys.stderr)
            
            # 映射颜色到节点名
            assignments = {}
            if success and coloring:
                for pod_name in all_pod_names:
                    idx = pod_to_idx[pod_name]
                    color = coloring.get(idx)
                    if color is not None:
                        node_idx = (color % num_nodes) + 1
                        node_name = f"node-{node_idx}"
                        assignments[pod_name] = node_name
                        print(f"[/batch_schedule] ✓ {pod_name} → {node_name} (颜色{color})", file=sys.stderr)
                
                print(f"[/batch_schedule] 📊 成功分配 {len(assignments)} 个Pod，使用{actual_colors}色", file=sys.stderr)
            
            return {
                "success": bool(assignments),
                "assignments": assignments,
                "colors_used": actual_colors,
                "attempts": attempts
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
