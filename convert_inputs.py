#!/usr/bin/env python3
"""
将 input/*.json 图数据转换为 TEST_INPUT/*.yaml Kubernetes 测试文件
"""
import json
import os
from pathlib import Path

def json_to_k8s_yaml(json_path: Path, output_dir: Path):
    """将单个JSON图文件转换为Kubernetes YAML"""
    
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    name = data['name']
    nodes_count = data['nodes']
    edges = data['edges']
    city_names = data.get('city_names', {})
    description = data.get('description', '')
    
    # 构建邻接表（用于生成anti-affinity规则）
    adjacency = {i: [] for i in range(nodes_count)}
    for u, v in edges:
        adjacency[u].append(v)
        adjacency[v].append(u)
    
    # 生成YAML内容
    yaml_lines = []
    yaml_lines.append(f"# {description}")
    yaml_lines.append(f"# {nodes_count}个Pod，{len(edges)}条冲突边")
    yaml_lines.append("")
    
    # 创建节点（至少与Pod数量相同）
    num_k8s_nodes = max(nodes_count, 5)  # 至少5个节点
    for i in range(1, num_k8s_nodes + 1):
        yaml_lines.extend([
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
            '    cpu: "8"',
            "    memory: 16Gi",
            '    pods: "110"',
            "  allocatable:",
            '    cpu: "8"',
            "    memory: 16Gi",
            '    pods: "110"',
        ])
    
    # 创建Pods
    for pod_idx in range(nodes_count):
        # 获取城市名仅用于注释
        city_name = city_names.get(str(pod_idx), f"pod-{pod_idx}")
        
        # 使用安全的Kubernetes名称 (RFC 1123)
        pod_name = f"pod-{pod_idx}"
        
        app_label = f"app-{pod_idx}"
        
        # 获取该Pod的所有冲突邻居
        neighbors = adjacency[pod_idx]
        neighbor_labels = [f"app-{n}" for n in neighbors]
        
        yaml_lines.extend([
            "---",
            f"# Pod {pod_idx}: {city_name} (连接: {', '.join(city_names.get(str(n), f'pod-{n}') for n in neighbors)})",
            "apiVersion: v1",
            "kind: Pod",
            "metadata:",
            f"  name: {pod_name}",
            "  labels:",
            f"    app: {app_label}",
            f'    quantum-batch: "{name}"',
            "  annotations:",
            f'    quantum-scheduler.io/batch-size: "{nodes_count}"',
            "spec:",
            "  schedulingGates:",
            '  - name: "quantum-scheduler.io/computing"',
            "  containers:",
            "  - name: app",
            "    image: nginx",
            "    resources:",
            "      requests:",
            '        cpu: "1"',
            "        memory: 1Gi",
        ])
        
        # 只有有邻居时才添加anti-affinity
        if neighbor_labels:
            yaml_lines.extend([
                "  affinity:",
                "    podAntiAffinity:",
                "      requiredDuringSchedulingIgnoredDuringExecution:",
                "      - labelSelector:",
                "          matchExpressions:",
                "          - key: app",
                "            operator: In",
                f"            values: {json.dumps(neighbor_labels)}",
                "        topologyKey: kubernetes.io/hostname",
            ])
    
    yaml_lines.append("")  # 结尾空行
    
    # 写入文件
    output_path = output_dir / f"{name}.yaml"
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(yaml_lines))
    
    print(f"✅ 转换完成: {json_path.name} -> {output_path.name}")
    return output_path

def main():
    # 路径设置
    input_dir = Path("/Users/hgn/Code/Python/Grover/input")
    output_dir = Path("/Users/hgn/Code/Python/Grover/Grover-kube-scheduler-simulator/TEST_INPUT")
    
    # 确保输出目录存在
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # 获取所有JSON文件
    json_files = sorted(input_dir.glob("*.json"))
    
    print(f"📂 找到 {len(json_files)} 个JSON文件")
    print(f"📤 输出目录: {output_dir}")
    print("-" * 60)
    
    # 批量转换
    converted_count = 0
    for json_file in json_files:
        try:
            json_to_k8s_yaml(json_file, output_dir)
            converted_count += 1
        except Exception as e:
            print(f"❌ 转换失败: {json_file.name} - {e}")
    
    print("-" * 60)
    print(f"🎉 转换完成！成功: {converted_count}/{len(json_files)}")

if __name__ == "__main__":
    main()
