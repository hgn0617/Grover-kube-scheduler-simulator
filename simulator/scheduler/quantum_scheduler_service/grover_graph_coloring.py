#!/usr/bin/env python3
"""
使用Grover算法解决图着色问题
完整的量子模拟实现
"""

import numpy as np
from qiskit import QuantumCircuit, QuantumRegister, ClassicalRegister
from qiskit_aer import AerSimulator
from qiskit.visualization import plot_histogram
import matplotlib.pyplot as plt
from pathlib import Path
import json
import math
import networkx as nx
import random
from graph_analysis import GraphColoringAnalyzer


class GroverGraphColoring:
    """使用Grover算法求解图着色问题"""
    
    def __init__(self, json_file, num_colors=None):
        """
        初始化求解器
        
        Args:
            json_file: 图的JSON文件路径
            num_colors: 使用的颜色数（None则自动判断）
        """
        # 分析图
        self.analyzer = GraphColoringAnalyzer(json_file)
        self.G, self.data = self.analyzer.get_graph_data()
        self.analysis = self.analyzer.analysis
        
        # 确定颜色数
        if num_colors is None:
            self.num_colors = self.analysis['recommended_colors']
        else:
            self.num_colors = num_colors
        
        # 图参数
        self.n_nodes = self.G.number_of_nodes()
        self.edges = list(self.G.edges())
        
        # 量子编码参数
        self.qubits_per_node = math.ceil(math.log2(self.num_colors))
        self.total_qubits = self.n_nodes * self.qubits_per_node
        
        # Grover参数
        self.search_space = self.num_colors ** self.n_nodes
        
        # 颜色编码
        self.color_encoding = self._build_encoding()
        
        print(f"初始化Grover图着色求解器")
        print(f"  图: {self.analysis['name']}")
        print(f"  节点数: {self.n_nodes}")
        print(f"  边数: {len(self.edges)}")
        print(f"  颜色数: {self.num_colors}")
        print(f"  量子位数: {self.total_qubits}")
        print(f"  有效颜色组合空间: {self.search_space:,}")
        
        # 计算迭代次数（会打印详细分析）
        self.grover_iterations = self._calculate_iterations()
    
    def _calculate_iterations(self):
        """
        计算最优Grover迭代次数（优化版）
        基于实际数据分析改进M预测准确性
        """
        # 计算搜索空间（量子态空间）
        main_qubits = self.n_nodes * self.qubits_per_node
        N = 2 ** main_qubits
        
        # 使用优化的M预测方法
        estimated_M = self._calculate_M_optimized()
        
        # Grover最优迭代次数：k = ⌊π/4 × √(N/M)⌋
        iterations = math.floor(math.pi / 4 * math.sqrt(N / estimated_M))
        
        # 限制在合理范围内
        iterations = max(2, min(iterations, 20))
        
        print(f"  搜索空间分析 (优化版):")
        print(f"    - 量子态搜索空间 N: {N} (2^{main_qubits})")
        print(f"    - 有效颜色组合: {self.num_colors ** self.n_nodes} ({self.num_colors}^{self.n_nodes})")
        print(f"    - 估计有效解数 M: ~{estimated_M}")
        print(f"    - 最优迭代次数 k: {iterations} (基于√(N/M))")
        
        return iterations
    
    def _calculate_M_optimized(self):
        """
        优化的M预测方法（基于实际数据调优）
        
        改进点：
        1. 密集图的约束比率下调0.5倍
        2. 二分图特殊处理
        3. 完全图使用排列公式
        4. 考虑聚类系数
        
        Returns:
            estimated_M: 估计的有效解数量
        """
        n_nodes = self.n_nodes
        n_edges = len(self.edges)
        num_colors = self.num_colors
        
        # 计算搜索空间
        valid_combinations = num_colors ** n_nodes
        
        # 基础特征
        edge_density = n_edges / (n_nodes * (n_nodes - 1) / 2) if n_nodes > 1 else 0
        
        # 检查二分图
        is_bipartite = nx.is_bipartite(self.G)
        
        # 特殊情况1: 二分图
        if is_bipartite:
            if num_colors == 2:
                # 二分图恰好有2种有效的2-着色
                return 2
            else:
                # 颜色充裕的二分图
                estimated_M = int(valid_combinations * 0.03)
                return max(2, estimated_M)
        
        # 特殊情况2: 完全图或接近完全图
        max_edges = n_nodes * (n_nodes - 1) // 2
        if n_edges >= max_edges * 0.9:  # 90%以上的边
            # 完全图：K_n 用 n 色有 n! 种有效着色
            if num_colors == n_nodes:
                factorial_M = 1
                for i in range(1, n_nodes + 1):
                    factorial_M *= i
                return factorial_M
            elif num_colors > n_nodes:
                # 颜色多于节点数：P(k, n) = k!/(k-n)!
                perm_M = 1
                for i in range(num_colors, num_colors - n_nodes, -1):
                    perm_M *= i
                return perm_M
            else:
                # 颜色不足
                return 1
        
        # 一般情况：使用改进的公式
        theoretical_ratio = (1 - 1/num_colors) ** n_edges
        
        # 根据边密度调整（改进版：针对超高密度图上调系数）
        if edge_density < 0.3:
            # 稀疏图 - 数据显示预测较准确
            constraint_ratio = min(0.3, theoretical_ratio * 1.5)
        elif edge_density < 0.5:
            # 中低密度
            constraint_ratio = theoretical_ratio * 1.2
        elif edge_density < 0.7:
            # 中高密度 - 需要下调
            constraint_ratio = theoretical_ratio * 0.8
        elif edge_density < 0.8:
            # 高密度
            constraint_ratio = theoretical_ratio * 0.5
        else:
            # 非常高密度（>0.8）：原来0.5，数据表明被低估，上调到0.6
            constraint_ratio = theoretical_ratio * 0.6
        
        # 考虑聚类系数（密集局部结构）
        try:
            clustering = nx.average_clustering(self.G)
            if clustering > 0.7:
                # 非常高的聚类系数（如哑铃图）
                constraint_ratio *= 0.6
            elif clustering > 0.5:
                # 高聚类系数意味着更多局部约束
                constraint_ratio *= 0.8
        except:
            pass
        
        # 针对大型cycle图（所有点度为2，n==m，且n>=7）的小幅下调
        degrees = dict(self.G.degree())
        if all(d == 2 for d in degrees.values()) and n_nodes == n_edges and n_nodes >= 7:
            constraint_ratio *= 0.9
        
        # 限制在合理范围
        constraint_ratio = max(0.001, min(0.5, constraint_ratio))
        
        estimated_M = int(valid_combinations * constraint_ratio)
        return max(1, estimated_M)
    
    def _build_encoding(self):
        """构建颜色的二进制编码"""
        encoding = {}
        for color in range(self.num_colors):
            binary = format(color, f'0{self.qubits_per_node}b')
            encoding[color] = binary
        return encoding
    
    def get_node_qubits(self, node):
        """获取节点对应的量子位索引"""
        start = node * self.qubits_per_node
        end = start + self.qubits_per_node
        return list(range(start, end))
    
    def decode_bitstring(self, bitstring):
        """
        将比特串解码为着色方案
        如果包含无效颜色编码，返回None
        """
        coloring = {}
        for node in range(self.n_nodes):
            start = node * self.qubits_per_node
            end = start + self.qubits_per_node
            color_bits = bitstring[start:end]
            color = int(color_bits, 2)
            
            # 检查颜色是否有效
            if color >= self.num_colors:
                # 包含无效颜色编码
                return None
            
            coloring[node] = color
        return coloring
    
    def is_valid_coloring(self, coloring):
        """检查着色方案是否有效"""
        for u, v in self.edges:
            if coloring[u] == coloring[v]:
                return False
        return True
    
    def check_color_validity(self, qc, node_qubits, ancilla_qubit):
        """
        检查单个节点的颜色编码是否有效
        例如：3色问题中，11是无效的（只能是00,01,10）
        
        有效则ancilla_qubit为|1⟩，无效则为|0⟩
        """
        # 获取无效的编码模式
        invalid_encodings = []
        max_encoding = 2**self.qubits_per_node - 1
        for code in range(self.num_colors, max_encoding + 1):
            invalid_encodings.append(code)
        
        if not invalid_encodings:
            # 所有编码都有效（2^n刚好等于颜色数）
            qc.x(ancilla_qubit)  # 直接标记为有效
            return
        
        # 初始化为有效（|1⟩）
        qc.x(ancilla_qubit)
        
        # 对每个无效编码，如果匹配则翻转ancilla为|0⟩
        for invalid_code in invalid_encodings:
            # 检测特定的位模式
            # 例如：检测|11⟩需要两个量子位都是1
            control_qubits = []
            bits_to_flip = []
            
            for bit_idx in range(self.qubits_per_node):
                bit_value = (invalid_code >> bit_idx) & 1
                if bit_value == 1:
                    control_qubits.append(node_qubits[bit_idx])
                else:
                    # 需要先翻转这个位
                    bits_to_flip.append(node_qubits[bit_idx])
                    qc.x(node_qubits[bit_idx])
                    control_qubits.append(node_qubits[bit_idx])
            
            # 如果所有控制位都是1，则翻转ancilla
            if len(control_qubits) == 1:
                qc.cx(control_qubits[0], ancilla_qubit)
            elif len(control_qubits) > 1:
                qc.mcx(control_qubits, ancilla_qubit)
            
            # 恢复翻转的位
            for bit in bits_to_flip:
                qc.x(bit)
    
    def uncompute_color_validity(self, qc, node_qubits, ancilla_qubit):
        """反向操作，恢复颜色有效性检查的辅助位"""
        invalid_encodings = []
        max_encoding = 2**self.qubits_per_node - 1
        for code in range(self.num_colors, max_encoding + 1):
            invalid_encodings.append(code)
        
        if not invalid_encodings:
            qc.x(ancilla_qubit)
            return
        
        # 反向操作
        for invalid_code in reversed(invalid_encodings):
            control_qubits = []
            bits_to_flip = []
            
            for bit_idx in range(self.qubits_per_node):
                bit_value = (invalid_code >> bit_idx) & 1
                if bit_value == 1:
                    control_qubits.append(node_qubits[bit_idx])
                else:
                    bits_to_flip.append(node_qubits[bit_idx])
                    qc.x(node_qubits[bit_idx])
                    control_qubits.append(node_qubits[bit_idx])
            
            if len(control_qubits) == 1:
                qc.cx(control_qubits[0], ancilla_qubit)
            elif len(control_qubits) > 1:
                qc.mcx(control_qubits, ancilla_qubit)
            
            for bit in bits_to_flip:
                qc.x(bit)
        
        qc.x(ancilla_qubit)
    
    def build_oracle(self, qc, qubits, ancilla):
        """
        构建Oracle：标记所有有效的着色方案
        检查：
        1. 每个节点的颜色编码必须有效（<num_colors）
        2. 所有边的相邻节点颜色必须不同
        
        Args:
            qc: 量子电路
            qubits: 主量子位寄存器
            ancilla: 辅助量子位
        """
        # 需要的辅助量子位：
        # - n个节点的颜色有效性检查
        # - m条边的相邻性检查
        # - 1个最终标记位
        
        # 分配辅助量子位（已在build_circuit中确保足够）
        nodes_to_check = self.n_nodes
        edges_to_check_count = len(self.edges)
        validity_ancillas = ancilla[:nodes_to_check]
        edge_ancillas = ancilla[nodes_to_check:nodes_to_check + edges_to_check_count]
        final_ancilla = ancilla[-1]
        edges_to_check = self.edges[:edges_to_check_count]
        
        # 步骤1：检查每个节点的颜色编码是否有效
        for node_idx in range(nodes_to_check):
            node_qubits = self.get_node_qubits(node_idx)
            self.check_color_validity(qc, node_qubits, validity_ancillas[node_idx])
        
        # 步骤2：检查每条边的约束（颜色不同）
        for idx, (u, v) in enumerate(edges_to_check):
            u_qubits = self.get_node_qubits(u)
            v_qubits = self.get_node_qubits(v)
            edge_anc = edge_ancillas[idx]
            
            # 逻辑：edge_anc = 1 当且仅当 u和v颜色不同
            # 方法：先假设不同(X门)，然后检测是否所有位都相同，若相同则翻转回0
            qc.x(edge_anc)
            
            # 检查每一位是否相等，如果所有位都相等则翻转edge_anc为0
            # 使用MCX：如果所有位的XOR都是0（相等），则翻转
            xor_results = []
            for i in range(self.qubits_per_node):
                # 对u[i]和v[i]进行比较
                qc.cx(qubits[u_qubits[i]], qubits[v_qubits[i]])
                xor_results.append(qubits[v_qubits[i]])
            
            # 如果所有XOR结果都是0（即颜色相同），使用X门和MCX检测
            for qubit in xor_results:
                qc.x(qubit)
            
            # 如果所有位都是1（XOR都是0，即颜色相同），翻转edge_anc
            if len(xor_results) == 1:
                qc.cx(xor_results[0], edge_anc)
            elif len(xor_results) > 1:
                qc.mcx(xor_results, edge_anc)
            
            # 恢复X门
            for qubit in xor_results:
                qc.x(qubit)
            
            # 恢复XOR操作
            for i in reversed(range(self.qubits_per_node)):
                qc.cx(qubits[u_qubits[i]], qubits[v_qubits[i]])
        
        # 步骤3：所有检查都通过时标记解
        all_checks = list(validity_ancillas[:nodes_to_check]) + list(edge_ancillas[:edges_to_check_count])
        
        if len(all_checks) > 0:
            qc.mcx(all_checks, final_ancilla)
        
        # 标记相位
        qc.z(final_ancilla)
        
        # 步骤4：反向操作恢复辅助位
        if len(all_checks) > 0:
            qc.mcx(all_checks, final_ancilla)
        
        # 恢复边检查（反向操作）
        for idx in range(len(edges_to_check) - 1, -1, -1):
            u, v = edges_to_check[idx]
            u_qubits = self.get_node_qubits(u)
            v_qubits = self.get_node_qubits(v)
            edge_anc = edge_ancillas[idx]
            
            # 反向恢复XOR操作
            for i in range(self.qubits_per_node):
                qc.cx(qubits[u_qubits[i]], qubits[v_qubits[i]])
            
            # 恢复X门
            xor_results = [qubits[v_qubits[i]] for i in range(self.qubits_per_node)]
            for qubit in xor_results:
                qc.x(qubit)
            
            # 恢复MCX
            if len(xor_results) == 1:
                qc.cx(xor_results[0], edge_anc)
            elif len(xor_results) > 1:
                qc.mcx(xor_results, edge_anc)
            
            # 恢复X门
            for qubit in xor_results:
                qc.x(qubit)
            
            # 恢复XOR操作
            for i in reversed(range(self.qubits_per_node)):
                qc.cx(qubits[u_qubits[i]], qubits[v_qubits[i]])
            
            # 恢复初始X门
            qc.x(edge_anc)
        
        # 恢复颜色有效性检查
        for node_idx in reversed(range(nodes_to_check)):
            node_qubits = self.get_node_qubits(node_idx)
            self.uncompute_color_validity(qc, node_qubits, validity_ancillas[node_idx])
    
    def build_diffusion(self, qc, qubits):
        """
        构建Diffusion算子（关于均匀叠加态的反射）
        D = 2|s⟩⟨s| - I
        """
        # H^⊗n
        for q in qubits:
            qc.h(q)
        
        # X^⊗n
        for q in qubits:
            qc.x(q)
        
        # 多控制Z门
        if len(qubits) == 1:
            qc.z(qubits[0])
        elif len(qubits) == 2:
            qc.cz(qubits[0], qubits[1])
        else:
            # 使用多控制Z门
            qc.h(qubits[-1])
            qc.mcx(qubits[:-1], qubits[-1])
            qc.h(qubits[-1])
        
        # X^⊗n
        for q in qubits:
            qc.x(q)
        
        # H^⊗n
        for q in qubits:
            qc.h(q)
    
    def build_circuit_with_iterations(self, grover_iterations: int):
        """构建Grover电路（指定迭代次数）"""
        if grover_iterations < 0:
            raise ValueError("grover_iterations must be non-negative")

        # 创建量子寄存器
        main_qubits = QuantumRegister(self.total_qubits, 'q')
        # 辅助量子位：n个节点颜色有效性检查 + m条边约束检查 + 1个最终标记
        num_ancilla = self.n_nodes + len(self.edges) + 1
        ancilla_qubits = QuantumRegister(num_ancilla, 'anc')
        classical_bits = ClassicalRegister(self.total_qubits, 'c')
        
        qc = QuantumCircuit(main_qubits, ancilla_qubits, classical_bits)
        
        # 初始化：创建均匀叠加态
        for q in main_qubits:
            qc.h(q)
        
        # Grover迭代
        for _ in range(grover_iterations):
            # Oracle
            self.build_oracle(qc, main_qubits, ancilla_qubits)
            
            # Diffusion
            self.build_diffusion(qc, main_qubits)
        
        # 测量
        qc.measure(main_qubits, classical_bits)
        
        return qc

    def build_circuit(self):
        """构建完整的Grover电路（使用当前配置的迭代次数）"""
        return self.build_circuit_with_iterations(self.grover_iterations)
    
    def run_simulation(self, shots=1000):
        """
        运行量子模拟
        
        Args:
            shots: 测量次数
            
        Returns:
            results: 测量结果
        """
        print(f"\n开始构建量子电路...")
        qc = self.build_circuit()
        
        print(f"电路构建完成:")
        print(f"  - 总量子位: {qc.num_qubits}")
        print(f"  - 主量子位: {self.total_qubits}")
        print(f"  - 辅助量子位: {qc.num_qubits - self.total_qubits}")
        print(f"  - 电路深度: {qc.depth()}")
        print(f"  - 门数量: {len(qc.data)}")
        
        # 估算内存需求
        total_qubits = qc.num_qubits
        estimated_memory_gb = (2**total_qubits * 16) / (1024**3)
        
        print(f"\n量子模拟器选择:")
        print(f"  - 量子位数: {total_qubits}")
        print(f"  - 估算内存需求: {estimated_memory_gb:.2f} GB")
        
        # 使用Qiskit的自动模拟器选择（会根据电路大小自动选择最优方法）
        simulator = AerSimulator()
        print(f"  - 模拟器方法: 自动选择")
        
        print(f"\n开始量子模拟（{shots}次测量）...")
        
        try:
            # 执行
            job = simulator.run(qc, shots=shots)
            result = job.result()
            counts = result.get_counts()
            
            print(f"模拟完成！")
            
        except Exception as e:
            if 'memory' in str(e).lower():
                print(f"\n⚠️ 内存不足！尝试使用更节省内存的方法...")
                print(f"   建议：减少量子位数或选择更小的图进行测试")
                raise
            else:
                raise
        
        return counts, qc
    
    def run_with_collapse_simulation(self, max_attempts=100, final_shots=10000):
        """
        运行真实量子坍缩模拟：单次观测+验证+重试
        
        Args:
            max_attempts: 最大重试次数
            final_shots: 成功后的完整观测次数（设为0跳过验证）
            
        Returns:
            success: 是否找到有效解
            coloring: 有效的着色方案
            attempts: 尝试次数
            final_counts: 最终的完整测量结果
            qc: 量子电路
            solve_time_ms: 求解时间（毫秒，不含验证）
        """
        import time
        
        print(f"\n{'='*70}")
        print(f"🔬 量子坍缩模拟模式")
        print(f"{'='*70}")
        print(f"模拟真实量子计算：单次观测 → 验证 → 重试（如失败）")
        print(f"最大尝试次数: {max_attempts}")
        print(f"\n准备量子电路...")
        
        # 构建电路（只构建一次）
        qc = self.build_circuit()
        simulator = AerSimulator()
        
        print(f"电路已准备完成")
        print(f"  - 总量子位: {qc.num_qubits}")
        print(f"  - Grover迭代: {self.grover_iterations}")
        
        # 开始坍缩模拟
        print(f"\n{'='*70}")
        print(f"开始量子态坍缩模拟...")
        print(f"{'='*70}\n")
        
        # 开始计时（求解时间）
        solve_start_time = time.time()
        
        for attempt in range(1, max_attempts + 1):
            print(f"\n🎯 第 {attempt} 次量子测量（坍缩）")
            print(f"-" * 50)
            
            # 单次测量（模拟量子态坍缩）
            job = simulator.run(qc, shots=1)
            result = job.result()
            counts = result.get_counts()
            
            # 获取坍缩的状态
            bitstring = list(counts.keys())[0]
            bitstring_reversed = bitstring[::-1]
            
            print(f"  观测到的量子态: |{bitstring_reversed}⟩")
            
            # 解码
            coloring = self.decode_bitstring(bitstring_reversed)
            
            if coloring is None:
                print(f"  ❌ 无效颜色编码")
                print(f"  → 量子态坍缩到无效编码，重新运行电路...")
                continue
            
            # 验证着色方案
            is_valid = self.is_valid_coloring(coloring)
            
            if is_valid:
                # 记录求解时间（找到有效解的时刻）
                solve_time_ms = (time.time() - solve_start_time) * 1000
                
                print(f"  ✅ 找到有效解！")
                print(f"\n{'='*70}")
                print(f"🎉 量子坍缩成功！")
                print(f"{'='*70}")
                print(f"尝试次数: {attempt}")
                print(f"求解时间: {solve_time_ms:.3f}ms")
                print(f"成功率: {1/attempt*100:.2f}% (理论期望: ~{1/self.search_space*100:.4f}%)")
                
                # 显示着色方案
                print(f"\n找到的有效着色方案:")
                city_names = self.data.get('city_names', {})
                for node in range(self.n_nodes):
                    node_name = city_names.get(str(node), f"节点{node}")
                    color = coloring[node]
                    print(f"  {node_name}: 颜色{color}")
                
                # 成功后进行完整观测（可选）
                final_counts = None
                if final_shots > 0:
                    print(f"\n{'='*70}")
                    print(f"📊 执行完整量子测量（{final_shots}次）验证概率分布...")
                    print(f"{'='*70}\n")
                    
                    final_job = simulator.run(qc, shots=final_shots)
                    final_result = final_job.result()
                    final_counts = final_result.get_counts()
                    
                    print(f"✅ 完整测量完成！")
                
                # 返回时附带求解时间
                return True, coloring, attempt, final_counts, qc, solve_time_ms
            
            else:
                # 显示为什么失败
                violations = []
                for u, v in self.edges:
                    if coloring[u] == coloring[v]:
                        violations.append((u, v))
                
                print(f"  ❌ 违反约束")
                print(f"  违反的边: {violations[:3]}{'...' if len(violations) > 3 else ''}")
                print(f"  → 量子态坍缩到无效解，重新运行电路...")
        
        # 达到最大尝试次数
        solve_time_ms = (time.time() - solve_start_time) * 1000
        print(f"\n{'='*70}")
        print(f"⚠️  达到最大尝试次数 ({max_attempts})")
        print(f"{'='*70}")
        print(f"说明：Grover算法的成功概率取决于迭代次数和Oracle质量")
        print(f"      当前配置未能在 {max_attempts} 次坍缩中找到有效解")
        print(f"总耗时: {solve_time_ms:.3f}ms")
        
        return False, None, max_attempts, None, qc, solve_time_ms

    def run_with_bbht_collapse_simulation(
        self,
        max_attempts: int = 100,
        final_shots: int = 0,
        *,
        max_grover_iterations: int = 20,
        lambda_factor: float = 1.2,
        seed: int | None = None,
        verbose: bool = False,
    ):
        """
        BBHT风格的坍缩模拟：每次attempt随机选择Grover迭代次数并验证。

        说明：
        - 这里的“attempt”指：构建电路→单次测量(shots=1)→解码→验证。
        - 为控制电路深度，迭代次数会被限制在 [0, max_grover_iterations]。
        - 该实现用于工程验证与实验标定，不等价于理想模型下的完整BBHT最坏情况保证。
        """
        import time

        if max_attempts <= 0:
            return False, None, 0, None, None, 0.0
        if max_grover_iterations < 0:
            raise ValueError("max_grover_iterations must be non-negative")
        if lambda_factor <= 1.0:
            raise ValueError("lambda_factor must be > 1.0")

        rng = random.Random(seed)
        simulator = AerSimulator()

        if verbose:
            print(f"\n{'='*70}")
            print(f"🔬 BBHT坍缩模拟模式")
            print(f"{'='*70}")
            print(f"最大尝试次数: {max_attempts}")
            print(f"最大Grover迭代上限: {max_grover_iterations}")
            print(f"lambda_factor: {lambda_factor}")

        solve_start_time = time.time()

        # BBHT: m逐步增长，每次随机选 r ∈ [0, m-1]
        m = 1
        m_max = max_grover_iterations + 1  # r<=max_grover_iterations

        last_qc = None
        iterations_used: list[int] = []
        oracle_calls_total = 0

        # 供外部（如/service）记录实验指标
        self.last_bbht_stats = {
            "max_attempts": int(max_attempts),
            "max_grover_iterations": int(max_grover_iterations),
            "lambda_factor": float(lambda_factor),
            "seed": seed,
            "attempts_used": 0,
            "iterations_used": iterations_used,
            "oracle_calls": 0,
            "success": False,
        }

        for attempt in range(1, max_attempts + 1):
            r = rng.randrange(m) if m > 0 else 0
            iterations_used.append(int(r))
            oracle_calls_total += int(r)

            # 构建电路并单次测量
            qc = self.build_circuit_with_iterations(r)
            last_qc = qc

            job = simulator.run(qc, shots=1)
            result = job.result()
            counts = result.get_counts()

            bitstring = list(counts.keys())[0]
            bitstring_reversed = bitstring[::-1]

            coloring = self.decode_bitstring(bitstring_reversed)
            if coloring is not None and self.is_valid_coloring(coloring):
                solve_time_ms = (time.time() - solve_start_time) * 1000
                final_counts = None

                if final_shots > 0:
                    final_job = simulator.run(qc, shots=final_shots)
                    final_result = final_job.result()
                    final_counts = final_result.get_counts()

                if verbose:
                    print(f"✅ BBHT成功: attempt={attempt}, iterations={r}, time={solve_time_ms:.3f}ms")

                self.last_bbht_stats.update(
                    {
                        "attempts_used": int(attempt),
                        "oracle_calls": int(oracle_calls_total),
                        "success": True,
                    }
                )

                return True, coloring, attempt, final_counts, qc, solve_time_ms

            # 增大m
            m = min(m_max, int(math.ceil(lambda_factor * m)))

        solve_time_ms = (time.time() - solve_start_time) * 1000
        if verbose:
            print(f"⚠️  BBHT未找到解: attempts={max_attempts}, time={solve_time_ms:.3f}ms")

        self.last_bbht_stats.update(
            {
                "attempts_used": int(max_attempts),
                "oracle_calls": int(oracle_calls_total),
                "success": False,
            }
        )

        return False, None, max_attempts, None, last_qc, solve_time_ms
    
    def analyze_results(self, counts):
        """分析测量结果（移除后期验证，直接报告量子测量原始结果）"""
        print(f"\n{'='*70}")
        print(f"量子测量原始结果分析（无经典后处理）")
        print(f"{'='*70}")
        
        total_measurements = sum(counts.values())
        print(f"总测量次数: {total_measurements}")
        print(f"不同状态数: {len(counts)}")
        
        # 解码所有结果，不做筛选
        all_solutions = []
        
        for bitstring, count in counts.items():
            # 反转比特串（Qiskit使用小端序）
            bitstring_reversed = bitstring[::-1]
            coloring = self.decode_bitstring(bitstring_reversed)
            all_solutions.append((bitstring, count, coloring))
        
        # 按观测次数排序
        all_solutions.sort(key=lambda x: x[1], reverse=True)
        
        print(f"\n{'='*70}")
        print(f"量子测量到的状态（按观测次数排序，前20个）:")
        print(f"{'='*70}")
        
        # 显示前20个最常观测到的状态
        for idx, (bitstring, count, coloring) in enumerate(all_solutions[:20], 1):
            prob = count / total_measurements * 100
            print(f"\n状态 {idx} (观测 {count} 次, {prob:.2f}%):")
            print(f"  比特串: {bitstring[::-1]}")  # 转回正常顺序显示
            
            # 检查这个状态是否有效（仅用于信息展示，不影响结果）
            if coloring is None:
                print(f"  ⚠️  包含无效颜色编码")
                continue
            
            is_valid = self.is_valid_coloring(coloring)
            status = "✓ 有效解" if is_valid else "✗ 违反边约束"
            print(f"  [{status}]")
            
            # 显示着色方案
            city_names = self.data.get('city_names', {})
            for node in range(self.n_nodes):
                node_name = city_names.get(str(node), f"节点{node}")
                color = coloring[node]
                color_binary = self.color_encoding[color]
                print(f"    {node_name}: 颜色{color} (|{color_binary}⟩)")
            
            # 如果违反约束，显示违反的边
            if not is_valid:
                violations = []
                for u, v in self.edges:
                    if coloring[u] == coloring[v]:
                        violations.append((u, v))
                print(f"    违反约束的边: {violations[:3]}{'...' if len(violations) > 3 else ''}")
        
        if len(all_solutions) > 20:
            print(f"\n... 还有 {len(all_solutions)-20} 个其他观测状态")
        
        # 统计信息（仅供参考，不作为成功率指标）
        print(f"\n{'='*70}")
        print(f"统计信息（仅供参考）:")
        print(f"{'='*70}")
        
        valid_count = 0
        invalid_encoding_count = 0
        constraint_violation_count = 0
        
        for bitstring, count, coloring in all_solutions:
            if coloring is None:
                invalid_encoding_count += count
            elif not self.is_valid_coloring(coloring):
                constraint_violation_count += count
            else:
                valid_count += count
        
        print(f"包含无效颜色编码: {invalid_encoding_count} 次 ({invalid_encoding_count/total_measurements*100:.1f}%)")
        print(f"违反边约束: {constraint_violation_count} 次 ({constraint_violation_count/total_measurements*100:.1f}%)")
        print(f"满足所有约束: {valid_count} 次 ({valid_count/total_measurements*100:.1f}%)")
        print(f"\n说明: 以上统计仅用于评估Oracle性能，不代表算法成功率")
        
        return all_solutions
    
    def visualize_results(self, counts, save_path=None):
        """可视化测量结果"""
        # 设置中文字体
        plt.rcParams['font.sans-serif'] = ['Arial Unicode MS', 'SimHei', 'DejaVu Sans']
        plt.rcParams['axes.unicode_minus'] = False
        
        # 显示所有结果，按频率排序
        sorted_counts = dict(sorted(counts.items(), 
                                   key=lambda x: x[1], 
                                   reverse=True))
        
        # 根据结果数量动态调整图表大小
        num_results = len(sorted_counts)
        if num_results <= 20:
            figsize = (15, 8)
        elif num_results <= 50:
            figsize = (20, 10)
        elif num_results <= 100:
            figsize = (30, 12)
        else:
            figsize = (40, 15)
        
        fig = plot_histogram(sorted_counts, figsize=figsize)
        
        plt.suptitle(f"Grover Algorithm Results - {self.analysis['name']}\n"
                    f"Colors: {self.num_colors}, Iterations: {self.grover_iterations}\n"
                    f"Showing all {num_results} observed states",
                    fontsize=14, fontweight='bold')
        
        if save_path:
            plt.savefig(save_path, dpi=150, bbox_inches='tight')
            print(f"\n结果图表已保存: {save_path}")
            print(f"  包含 {num_results} 个观测状态")
        
        plt.close()
    
    def save_solution(self, all_solutions, total_shots, output_dir='./output'):
        """保存解决方案到文件（包含所有测量结果）"""
        if not all_solutions:
            return
        
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        # 统计有效解
        valid_solutions = []
        invalid_encoding_count = 0
        constraint_violation_count = 0
        
        for bitstring, count, coloring in all_solutions:
            if coloring is None:
                invalid_encoding_count += count
            elif not self.is_valid_coloring(coloring):
                constraint_violation_count += count
            else:
                valid_solutions.append((bitstring, count, coloring))
        
        total_valid_observations = sum([count for _, count, _ in valid_solutions])
        
        # 选择观测次数最多的状态
        best_solution = all_solutions[0]
        bitstring, count, coloring = best_solution
        best_is_valid = coloring is not None and self.is_valid_coloring(coloring)
        
        # 准备前20个观测状态列表
        top_solutions = []
        for idx, (bs, cnt, col) in enumerate(all_solutions[:20], 1):
            is_valid = col is not None and self.is_valid_coloring(col)
            solution_data = {
                'rank': idx,
                'bitstring': bs[::-1],
                'observations': cnt,
                'probability': f"{cnt/total_shots*100:.2f}%",
                'is_valid': is_valid
            }
            
            if col is not None:
                solution_data['coloring'] = col
                solution_data['colored_nodes'] = {
                    self.data.get('city_names', {}).get(str(node), f'Node{node}'): color
                    for node, color in col.items()
                }
            else:
                solution_data['error'] = 'Invalid color encoding'
            
            top_solutions.append(solution_data)
        
        # 准备输出数据
        output_data = {
            'graph_name': self.analysis['name'],
            'num_nodes': self.n_nodes,
            'num_edges': len(self.edges),
            'num_colors': self.num_colors,
            'grover_iterations': self.grover_iterations,
            'total_qubits': self.total_qubits,
            'search_space': self.search_space,
            'measurement_info': {
                'total_shots': total_shots,
                'different_states': len(all_solutions),
                'oracle_performance': {
                    'valid_observations': total_valid_observations,
                    'valid_percentage': f"{total_valid_observations/total_shots*100:.2f}%",
                    'invalid_encoding': invalid_encoding_count,
                    'constraint_violations': constraint_violation_count
                }
            },
            'most_observed_state': {
                'bitstring': bitstring[::-1],
                'observations': count,
                'probability': f"{count/total_shots*100:.2f}%",
                'is_valid': best_is_valid,
                'coloring': coloring if coloring is not None else None,
                'colored_nodes': {
                    self.data.get('city_names', {}).get(str(node), f'Node{node}'): color
                    for node, color in coloring.items()
                } if coloring is not None else None
            },
            'top_20_observed_states': top_solutions,
            'note': 'This file contains raw quantum measurement results without classical post-processing'
        }
        
        # 保存JSON
        json_file = output_path / f"{self.analysis['name']}_solution.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)
        
        print(f"测量结果已保存: {json_file}")


def solve_graph_coloring(json_file, num_colors=None, shots=1000, 
                         visualize=True, save=True):
    """
    使用Grover算法求解单个图的着色问题（传统模式）
    
    Args:
        json_file: 图的JSON文件
        num_colors: 使用的颜色数
        shots: 测量次数
        visualize: 是否可视化结果
        save: 是否保存结果
    """
    print(f"\n{'='*70}")
    print(f"求解图着色问题: {Path(json_file).stem}")
    print(f"{'='*70}")
    
    # 创建求解器
    solver = GroverGraphColoring(json_file, num_colors)
    
    # 运行模拟
    counts, circuit = solver.run_simulation(shots)
    
    # 分析结果（不做后处理过滤）
    all_solutions = solver.analyze_results(counts)
    
    # 可视化
    if visualize:
        graph_name = solver.analysis['name']
        vis_path = f"./output/{graph_name}_grover_results.png"
        solver.visualize_results(counts, vis_path)
    
    # 保存测量结果
    if save and all_solutions:
        solver.save_solution(all_solutions, shots)
    
    return solver, counts, all_solutions


def solve_with_collapse(json_file, num_colors=None, max_attempts=100, 
                        final_shots=10000, visualize=True, save=True):
    """
    使用量子坍缩模拟模式求解图着色问题
    模拟真实量子计算：单次观测→验证→重试
    
    Args:
        json_file: 图的JSON文件
        num_colors: 使用的颜色数
        max_attempts: 最大重试次数
        final_shots: 成功后的完整观测次数
        visualize: 是否可视化结果
        save: 是否保存结果
        
    Returns:
        solver: 求解器实例
        success: 是否成功
        coloring: 找到的着色方案
        attempts: 尝试次数
        final_counts: 完整测量结果
    """
    print(f"\n{'='*70}")
    print(f"量子坍缩模式求解: {Path(json_file).stem}")
    print(f"{'='*70}")
    
    # 创建求解器
    solver = GroverGraphColoring(json_file, num_colors)
    
    # 运行坍缩模拟
    success, coloring, attempts, final_counts, circuit, solve_time_ms = solver.run_with_collapse_simulation(
        max_attempts=max_attempts, 
        final_shots=final_shots
    )
    
    if success:
        # 分析完整测量结果
        print(f"\n{'='*70}")
        print(f"完整测量结果分析 ({final_shots}次观测)")
        print(f"{'='*70}")
        all_solutions = solver.analyze_results(final_counts)
        
        # 可视化
        if visualize:
            graph_name = solver.analysis['name']
            vis_path = f"./output/{graph_name}_collapse_results.png"
            solver.visualize_results(final_counts, vis_path)
        
        # 保存结果
        if save and all_solutions:
            solver.save_solution(all_solutions, final_shots)
            
            # 额外保存坍缩信息
            output_path = Path('./output')
            collapse_info = {
                'mode': 'quantum_collapse_simulation',
                'success': True,
                'attempts_needed': attempts,
                'max_attempts': max_attempts,
                'success_rate': f"{1/attempts*100:.2f}%",
                'found_coloring': coloring,
                'final_measurement_shots': final_shots
            }
            
            info_file = output_path / f"{solver.analysis['name']}_collapse_info.json"
            with open(info_file, 'w', encoding='utf-8') as f:
                json.dump(collapse_info, f, ensure_ascii=False, indent=2)
            print(f"\n坍缩模拟信息已保存: {info_file}")
    else:
        print(f"\n未能在 {max_attempts} 次尝试中找到有效解")
        all_solutions = None
    
    return solver, success, coloring, attempts, final_counts


# 注意：批量求解功能已移至 run_grover_solver.py
# 该文件提供了更完善的交互式界面、超时控制和内存检查
# 请使用: python run_grover_solver.py
