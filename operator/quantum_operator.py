#!/usr/bin/env python3
"""
Quantum Batch Operator
监听Pod创建事件，识别批次，调用量子服务全局计算，写入Annotation
"""

import time
import json
import subprocess
import requests
from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException

class QuantumBatchOperator:
    def __init__(self, quantum_service_url="http://localhost:8000"):
        self.quantum_service_url = quantum_service_url
        self.batches = {}  # {"batch-id": {"pods": [...], "expected_size": 5, "status": "waiting"}}
        
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
        gates = getattr(pod.spec, "scheduling_gates", None) or []

        # 如果已经有推荐节点且没有调度门，说明处理过了，直接跳过
        if 'quantum-scheduler.io/recommended-node' in annotations and not gates:
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
        
        # 1. 构建请求数据
        conflicts_map = self.extract_conflicts(pods)
        
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
            "num_nodes": 5  # 假设5个节点
        }
        
        print(f"[Operator] 调用量子服务: {self.quantum_service_url}/batch_schedule")
        
        try:
            # 2. 调用量子服务（计时开始）
            quantum_start_time = time.time()
            resp = requests.post(
                f"{self.quantum_service_url}/batch_schedule",
                json=payload,
                timeout=180
            )
            resp.raise_for_status()
            result = resp.json()
            quantum_end_time = time.time()
            quantum_elapsed = quantum_end_time - quantum_start_time
            
            if not result.get('success'):
                print(f"[Operator] ❌ 量子计算失败: {result.get('error')}")
                batch['status'] = 'failed'
                return
            
            assignments = result.get('assignments', {})
            colors_used = result.get('colors_used', 0)
            
            print(f"[Operator] ✓ 量子计算成功！使用 {colors_used} 种颜色")
            print(f"[Operator] ⏱️  量子计算耗时: {quantum_elapsed:.3f}s")
            
            # 3. 两阶段提交：先写annotation，再统一清除gates
            print(f"[Operator] 阶段1: 写入推荐节点 annotations 并清除 gates...")
            for pod in pods:
                pod_name = pod.metadata.name
                recommended_node = assignments.get(pod_name)
                
                if recommended_node:
                    # 同步写 annotation 并清除 gates，避免队列卡住
                    success = self.annotate_pod(pod_name, recommended_node, clear_gates=True)
                    if success:
                        print(f"[Operator]   ✓ {pod_name} → {recommended_node} (annotation已写入, gates已清除)")
                    else:
                        print(f"[Operator]   ❌ {pod_name} annotation 写入失败")
            
            print(f"[Operator] 阶段2: 统一清除所有 schedulingGates(幂等保障)...")
            # 等所有 annotation 写完，再一次性清除所有 gates
            for pod in pods:
                pod_name = pod.metadata.name
                success = self.clear_scheduling_gates(pod_name)
                if success:
                    print(f"[Operator]   ✓ {pod_name} gates已清除")
            
            batch['status'] = 'completed'
            print(f"[Operator] 🎉 Batch '{batch_id}' 处理完成！")
        
        except requests.exceptions.Timeout:
            print(f"[Operator] ❌ 量子服务超时")
            batch['status'] = 'timeout'
        except Exception as e:
            print(f"[Operator] ❌ 处理批次时出错: {e}")
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


if __name__ == "__main__":
    operator = QuantumBatchOperator()
    operator.run()
