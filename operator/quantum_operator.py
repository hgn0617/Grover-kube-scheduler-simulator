#!/usr/bin/env python3
"""
Quantum Batch Operator
监听Pod创建事件，识别批次，调用量子服务全局计算，写入Annotation
"""

import time
import json
import subprocess
import requests
import os
from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException

class QuantumBatchOperator:
    def __init__(self, quantum_service_url="http://localhost:8000"):
        # 默认本机；如需远程量子服务，可设置 QUANTUM_SERVICE_URL
        self.quantum_service_url = os.getenv("QUANTUM_SERVICE_URL", quantum_service_url)
        self.batches = {}  # {"batch-id": {"pods": [...], "expected_size": 5, "status": "waiting"}}

        # 可通过环境变量覆盖（用于实验）
        self.node_budget = int(os.getenv("QUANTUM_NODE_BUDGET", "5"))
        self.attempt_budget = int(os.getenv("QUANTUM_ATTEMPT_BUDGET", "8"))
        self.max_grover_iterations = int(os.getenv("QUANTUM_MAX_GROVER_ITERATIONS", "5"))
        self.bbht_lambda = float(os.getenv("QUANTUM_BBHT_LAMBDA", "1.2"))
        seed_env = os.getenv("QUANTUM_SEED")
        self.seed = int(seed_env) if seed_env is not None and seed_env != "" else None

        print(
            "[Operator] 参数: "
            f"QUANTUM_SERVICE_URL={self.quantum_service_url}, "
            f"QUANTUM_NODE_BUDGET={self.node_budget}, "
            f"QUANTUM_ATTEMPT_BUDGET={self.attempt_budget}, "
            f"QUANTUM_MAX_GROVER_ITERATIONS={self.max_grover_iterations}, "
            f"QUANTUM_BBHT_LAMBDA={self.bbht_lambda}, "
            f"QUANTUM_SEED={self.seed}"
        )
        
        # 初始化K8s客户端 - 使用更简单的配置
        print("[Operator] 初始化 K8s 客户端...")
        
        # 直接配置连接模拟器
        configuration = client.Configuration()
        configuration.host = "http://localhost:3131"
        configuration.verify_ssl = False
        configuration.debug = False
        
        # 设置默认配置
        client.Configuration.set_default(configuration)
        self.v1 = client.CoreV1Api()
        
        # 测试连接
        try:
            pods = self.v1.list_namespaced_pod(namespace="default", limit=1)
            print(f"[Operator] ✓ 成功连接到模拟器，发现 {len(pods.items)} 个Pod")
            print("[Operator] Quantum Batch Operator 已启动")
        except Exception as e:
            print(f"[Operator] ❌ 连接失败: {e}")
            print(f"[Operator] 错误类型: {type(e).__name__}")
            import traceback
            traceback.print_exc()
            raise
    
    def run(self):
        """持续监听Pod创建事件"""
        print("[Operator] 开始监听 Pod 事件...")
        w = watch.Watch()
        
        while True:
            try:
                for event in w.stream(self.v1.list_namespaced_pod, namespace="default", timeout_seconds=0):
                    event_type = event['type']
                    pod = event['object']

                    if event_type in ['ADDED', 'MODIFIED']:
                        name = pod.metadata.name
                        phase = pod.status.phase
                        node_name = pod.spec.node_name or "<none>"
                        gates = getattr(pod.spec, "scheduling_gates", None) or []
                        print(f"[Operator] 事件: {event_type} Pod={name}, phase={phase}, node={node_name}, gates={len(gates)}")
                        self.handle_pod(pod)
                        
            except Exception as e:
                print(f"[Operator] Watch错误: {e}, 5秒后重试...")
                time.sleep(5)
    
    def handle_pod(self, pod):
        """处理单个Pod"""
        name = pod.metadata.name
        annotations = pod.metadata.annotations or {}

        # 如果已经有推荐节点 annotation，说明这个 pod 已经被处理过了，直接跳过。
        # 不要求 gates 已清除——phase 1（写 annotation）与 phase 2（清 gates）会分别触发
        # MODIFIED 事件，其中 phase 1 事件的快照是 (annotation=yes, gates=yes)。若仅以
        # "annotation + 无 gates" 判定已处理，这些事件会把已 completed 的 batch 重置并
        # 重新收集 pod，导致每个 batch 被 process_batch 重复调用一次，产生重复量子计算。
        if 'quantum-scheduler.io/recommended-node' in annotations:
            return

        # 检查是否有batch标签
        if not pod.metadata.labels:
            return
        
        batch_id = pod.metadata.labels.get('quantum-batch')
        if not batch_id:
            return  # 不是量子批次
        
        # 获取预期Pod数量
        if not annotations:
            return
        
        batch_size_str = annotations.get('quantum-scheduler.io/batch-size', '0')
        try:
            expected_size = int(batch_size_str)
        except:
            expected_size = 0
        
        if expected_size <= 0:
            return
        
        # 初始化/重置 batch 跟踪
        # 场景：同一个 batch-id（如 'barbell'）可能会被多次 apply
        # 当上一轮状态是 completed/failed/timeout/error 时，要为新一轮重新创建 batch
        reset_statuses = {'completed', 'failed', 'timeout', 'error'}
        if batch_id not in self.batches or self.batches[batch_id].get('status') in reset_statuses:
            self.batches[batch_id] = {
                'pods': [],
                'expected_size': expected_size,
                'status': 'waiting'
            }

        batch = self.batches[batch_id]
        
        # 检查Pod是否已加入
        pod_names = [p.metadata.name for p in batch['pods']]
        if pod.metadata.name not in pod_names:
            batch['pods'].append(pod)
            print(f"[Operator] Batch '{batch_id}': {len(batch['pods'])}/{expected_size} pods 已到达 (新Pod={name})")
        
        # 检查是否所有Pod都到齐了
        if len(batch['pods']) == expected_size and batch['status'] == 'waiting':
            print(f"[Operator] ✓ Batch '{batch_id}' 完整！开始全局量子计算...")
            self.process_batch(batch_id)
    
    def process_batch(self, batch_id):
        """对完整批次调用量子服务"""
        batch = self.batches[batch_id]
        batch['status'] = 'processing'
        
        pods = batch['pods']
        process_start = time.perf_counter()
        quantum_start_clock = None
        metrics = {
            "batch_id": batch_id,
            "pod_count": len(pods),
            "status": "processing",
        }
        
        # 1. 构建请求数据
        extract_start = time.perf_counter()
        conflicts_map = self.extract_conflicts(pods)
        extract_conflicts_ms = (time.perf_counter() - extract_start) * 1000.0
        conflict_edges = set()
        for pod_name, conflicts in conflicts_map.items():
            for other in conflicts:
                conflict_edges.add(tuple(sorted((pod_name, other))))
        metrics["conflict_edges"] = len(conflict_edges)
        metrics["extract_conflicts_ms"] = round(extract_conflicts_ms, 3)
        
        print(f"[Operator] 提取的冲突关系:")
        for pod_name, conflicts in conflicts_map.items():
            print(f"[Operator]   {pod_name} -> {conflicts}")
        
        payload = {
            "pods": [
                {
                    "pod_name": pod.metadata.name,
                    "conflicts_with": conflicts_map.get(pod.metadata.name, [])
                }
                for pod in pods
            ],
            "graph_name": batch_id,
            "num_nodes": self.node_budget,
            "attempt_budget": self.attempt_budget,
            "max_grover_iterations": self.max_grover_iterations,
            "bbht_lambda": self.bbht_lambda,
            "seed": self.seed,
        }
        
        print(f"[Operator] 调用量子服务: {self.quantum_service_url}/batch_schedule")
        
        try:
            # 2. 调用量子服务（计时开始）
            quantum_start_clock = time.perf_counter()
            quantum_start_time = time.time()
            resp = requests.post(
                f"{self.quantum_service_url}/batch_schedule",
                json=payload,
                timeout=7200  # 2小时超时
            )
            resp.raise_for_status()
            result = resp.json()
            quantum_end_time = time.time()
            quantum_elapsed = quantum_end_time - quantum_start_time
            quantum_rpc_ms = (time.perf_counter() - quantum_start_clock) * 1000.0
            metrics["quantum_rpc_ms"] = round(quantum_rpc_ms, 3)
            
            if not result.get('success'):
                print(f"[Operator] ❌ 量子计算失败: {result.get('error')}")
                metrics["status"] = "quantum_failed"
                metrics["error"] = result.get("error")
                metrics["operator_total_ms"] = round((time.perf_counter() - process_start) * 1000.0, 3)
                self.log_metrics(metrics)
                batch['status'] = 'failed'
                return
            
            assignments = result.get('assignments', {})
            colors_used = result.get('colors_used', 0)
            k_upper = result.get("k_upper")
            k_start = result.get("k_start")
            k_found = result.get("k_found")
            attempts_total = result.get("attempts")
            oracle_calls = result.get("oracle_calls")
            attempts_by_k = result.get("attempts_by_k") or {}
            oracle_calls_by_k = result.get("oracle_calls_by_k") or {}
            metrics["colors_used"] = colors_used
            metrics["k_upper"] = k_upper
            metrics["k_start"] = k_start
            metrics["k_found"] = k_found
            metrics["attempts"] = attempts_total
            metrics["oracle_calls"] = oracle_calls
            
            print(f"[Operator] ✓ 量子计算成功！使用 {colors_used} 种颜色")
            print(f"[Operator] ⏱️  量子计算耗时: {quantum_elapsed:.3f}s")
            if k_upper is not None and k_found is not None:
                budgets_tested = len(attempts_by_k) if isinstance(attempts_by_k, dict) else 0
                k_min_tried = None
                if isinstance(attempts_by_k, dict) and attempts_by_k:
                    try:
                        k_min_tried = min(int(x) for x in attempts_by_k.keys())
                    except Exception:
                        k_min_tried = None

                successful_descents = int(k_start) - int(k_found) if k_start is not None else None
                print(
                    f"[Operator]   量子下降汇总: 贪心上界k_u={k_upper}, 起始k_start={k_start}, "
                    f"最终k_found={k_found}, 成功下降步数={successful_descents}, "
                    f"测试预算数={budgets_tested}, 最低尝试k={k_min_tried}"
                )
                if isinstance(attempts_by_k, dict) and attempts_by_k:
                    print(f"[Operator]   量子下降细节: attempts_by_k={attempts_by_k}, oracle_calls_by_k={oracle_calls_by_k}")
            if attempts_total is not None or oracle_calls is not None:
                print(f"[Operator]   指标: attempts={attempts_total}, oracle_calls={oracle_calls}")
            
            # 3. 两阶段提交（严格顺序，消除 race condition）：
            #    阶段1：只写 annotation，保持 scheduling gates 阻挡调度
            #    阶段2：等全部 annotation 落地后再统一清除 gates
            # 关键：不要在阶段1 同时清除 gates！否则第一个 pod 会在后续 pod
            # 的 annotation 写入前就被调度器接管，陷入 Priority 2/3 路径。
            print(f"[Operator] 阶段1: 写入所有推荐节点 annotations（不清 gates）...")
            annotate_start = time.perf_counter()
            annotated_pods = []
            for pod in pods:
                pod_name = pod.metadata.name
                recommended_node = assignments.get(pod_name)
                
                if recommended_node:
                    success = self.annotate_pod(pod_name, recommended_node, clear_gates=False)
                    if success:
                        annotated_pods.append(pod_name)
                        print(f"[Operator]   ✓ {pod_name} → {recommended_node} (annotation已写入)")
                    else:
                        print(f"[Operator]   ❌ {pod_name} annotation 写入失败")
            annotate_apply_ms = (time.perf_counter() - annotate_start) * 1000.0
            metrics["annotate_apply_ms"] = round(annotate_apply_ms, 3)
            
            print(f"[Operator] 阶段2: 所有 annotation 就位，统一清除 scheduling gates...")
            # 此时每个 pod 的 annotation 都已在 API server 上，清除 gates 后
            # scheduler 读到的 pod snapshot 必然包含 annotation，Priority 1 生效。
            clear_start = time.perf_counter()
            for pod in pods:
                pod_name = pod.metadata.name
                success = self.clear_scheduling_gates(pod_name)
                if success:
                    print(f"[Operator]   ✓ {pod_name} gates已清除")
            clear_gates_ms = (time.perf_counter() - clear_start) * 1000.0
            metrics["clear_gates_ms"] = round(clear_gates_ms, 3)
            
            batch['status'] = 'completed'
            metrics["status"] = "completed"
            metrics["operator_total_ms"] = round((time.perf_counter() - process_start) * 1000.0, 3)
            self.log_metrics(metrics)
            print(f"[Operator] 🎉 Batch '{batch_id}' 处理完成！")
        
        except requests.exceptions.Timeout:
            print(f"[Operator] ❌ 量子服务超时")
            metrics["status"] = "timeout"
            if quantum_start_clock is not None:
                metrics["quantum_rpc_ms"] = round((time.perf_counter() - quantum_start_clock) * 1000.0, 3)
            metrics["operator_total_ms"] = round((time.perf_counter() - process_start) * 1000.0, 3)
            self.log_metrics(metrics)
            batch['status'] = 'timeout'
        except Exception as e:
            print(f"[Operator] ❌ 处理批次时出错: {e}")
            metrics["status"] = "error"
            metrics["error"] = str(e)
            if quantum_start_clock is not None:
                metrics["quantum_rpc_ms"] = round((time.perf_counter() - quantum_start_clock) * 1000.0, 3)
            metrics["operator_total_ms"] = round((time.perf_counter() - process_start) * 1000.0, 3)
            self.log_metrics(metrics)
            batch['status'] = 'error'
    
    def extract_conflicts(self, pods):
        """从Pod的反亲和性提取冲突关系"""
        conflicts = {}
        
        # 构建 app 标签到 pod 名称的映射
        app_to_pod = {}
        for pod in pods:
            if pod.metadata.labels and 'app' in pod.metadata.labels:
                app_label = pod.metadata.labels['app']
                app_to_pod[app_label] = pod.metadata.name
        
        for pod in pods:
            pod_name = pod.metadata.name
            conflicts[pod_name] = []
            
            # 解析podAntiAffinity
            if not pod.spec.affinity:
                continue
            
            affinity = pod.spec.affinity
            if not affinity.pod_anti_affinity:
                continue
            
            required_terms = affinity.pod_anti_affinity.required_during_scheduling_ignored_during_execution
            if not required_terms:
                continue
            
            for term in required_terms:
                if not term.label_selector:
                    continue
                
                if not term.label_selector.match_expressions:
                    continue
                
                for expr in term.label_selector.match_expressions:
                    if expr.operator == "In" and expr.values:
                        # 将 app 标签映射回 pod 名称
                        for app_label in expr.values:
                            if app_label in app_to_pod:
                                conflicts[pod_name].append(app_to_pod[app_label])
        
        return conflicts
    
    def annotate_pod(self, pod_name, recommended_node, clear_gates=True):
        """给Pod添加推荐节点的Annotation
        
        Args:
            pod_name: Pod名称
            recommended_node: 推荐的节点
            clear_gates: 是否同时清除 schedulingGates（默认True保持向后兼容）
        """
        try:
            # 构建 patch
            patch = {
                "metadata": {
                    "annotations": {
                        "quantum-scheduler.io/recommended-node": recommended_node
                    }
                }
            }
            
            # 可选：同时清除 schedulingGates
            if clear_gates:
                patch["spec"] = {
                    "schedulingGates": []
                }

            result = subprocess.run(
                [
                    "kubectl", "--server=http://localhost:3131",
                    "patch", "pod", pod_name,
                    "--type=merge", "-p", json.dumps(patch)
                ],
                capture_output=True,
                text=True,
            )

            if result.returncode == 0:
                return True
            else:
                print(f"[Operator] kubectl patch 失败: {result.stderr.strip()[:200]}")
                return False

        except Exception as e:
            print(f"[Operator] Patch异常: {e}")
            return False
    
    def clear_scheduling_gates(self, pod_name):
        """清除Pod的schedulingGates"""
        try:
            patch = {
                "spec": {
                    "schedulingGates": []
                }
            }
            
            result = subprocess.run(
                [
                    "kubectl", "--server=http://localhost:3131",
                    "patch", "pod", pod_name,
                    "--type=merge", "-p", json.dumps(patch)
                ],
                capture_output=True,
                text=True,
            )
            
            if result.returncode == 0:
                return True
            else:
                print(f"[Operator] 清除gates失败: {result.stderr.strip()[:200]}")
                return False
        
        except Exception as e:
            print(f"[Operator] 清除gates异常: {e}")
            return False

    def log_metrics(self, metrics):
        """Emit one structured metrics line for the test harness."""
        print(f"[Operator][Metrics] {json.dumps(metrics, ensure_ascii=False, sort_keys=True)}")


if __name__ == "__main__":
    operator = QuantumBatchOperator()
    operator.run()
