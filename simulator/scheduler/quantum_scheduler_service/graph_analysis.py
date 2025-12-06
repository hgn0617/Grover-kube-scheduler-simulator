#!/usr/bin/env python3
"""
图着色问题分析工具
自动分析图的色数范围和最优参数
"""

import json
import networkx as nx
from pathlib import Path
import math

class GraphColoringAnalyzer:
    """图着色分析器"""
    
    def __init__(self, json_file):
        """加载并分析图"""
        self.json_file = json_file
        self.data = self._load_graph()
        self.G = self._build_networkx_graph()
        self.analysis = self._analyze()
    
    def _load_graph(self):
        """从JSON加载图数据"""
        with open(self.json_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def _build_networkx_graph(self):
        """构建NetworkX图对象"""
        G = nx.Graph()
        G.add_nodes_from(range(self.data['nodes']))
        G.add_edges_from(self.data['edges'])
        return G
    
    def _analyze(self):
        """完整分析图的色数和参数"""
        n = self.G.number_of_nodes()
        m = self.G.number_of_edges()
        
        analysis = {
            'name': self.data['name'],
            'nodes': n,
            'edges': m,
            'city_names': self.data.get('city_names', {}),
        }
        
        # 检查是否是二分图（2-着色）
        is_bipartite = nx.is_bipartite(self.G)
        analysis['is_bipartite'] = is_bipartite
        
        if is_bipartite:
            # 二分图色数为2
            analysis['chromatic_lower'] = 2
            analysis['chromatic_upper'] = 2
            analysis['chromatic_number'] = 2
            analysis['recommended_colors'] = 2
        else:
            # 计算各种界限
            # 下界1：最大团大小
            try:
                cliques = list(nx.find_cliques(self.G))
                max_clique = max(cliques, key=len)
                clique_size = len(max_clique)
            except:
                clique_size = 1
            
            # 下界2：基于边密度
            density_lower = math.ceil(2 * m / n) if n > 0 else 1
            
            # 最大度数
            degrees = dict(self.G.degree())
            max_degree = max(degrees.values()) if degrees else 0
            
            # 上界：Brooks定理（最大度数+1）
            brooks_upper = max_degree + 1
            
            # 贪心着色（实际上界）
            coloring = nx.greedy_color(self.G, strategy='largest_first')
            greedy_colors = max(coloring.values()) + 1
            
            # 确定下界和上界
            lower = max(clique_size, density_lower)
            upper = min(greedy_colors, brooks_upper, n)
            
            analysis['max_clique_size'] = clique_size
            analysis['max_degree'] = max_degree
            analysis['greedy_colors'] = greedy_colors
            analysis['chromatic_lower'] = lower
            analysis['chromatic_upper'] = upper
            
            # 推荐使用贪心算法得到的颜色数（通常接近最优）
            analysis['recommended_colors'] = greedy_colors
            analysis['chromatic_number'] = greedy_colors  # 近似值
        
        return analysis
    
    def print_analysis(self):
        """打印分析结果"""
        a = self.analysis
        print(f"\n{'='*70}")
        print(f"图分析: {a['name']}")
        print(f"{'='*70}")
        print(f"节点数: {a['nodes']}")
        print(f"边数: {a['edges']}")
        
        if a.get('is_bipartite'):
            print(f"类型: 二分图 ✓")
            print(f"色数: 2 (确定)")
        else:
            print(f"类型: 一般图")
            print(f"色数范围: [{a['chromatic_lower']}, {a['chromatic_upper']}]")
            print(f"最大团大小: {a['max_clique_size']}")
            print(f"最大度数: {a['max_degree']}")
            print(f"贪心着色结果: {a['greedy_colors']} 种颜色")
        
        print(f"\n推荐使用颜色数: {a['recommended_colors']}")
        print(f"{'='*70}\n")
    
    def get_graph_data(self):
        """返回图数据"""
        return self.G, self.data


def analyze_all_graphs(input_dir='./input'):
    """分析所有图文件"""
    input_path = Path(input_dir)
    json_files = sorted(input_path.glob('*.json'))
    
    print(f"\n找到 {len(json_files)} 个图文件")
    
    results = []
    for json_file in json_files:
        analyzer = GraphColoringAnalyzer(json_file)
        analyzer.print_analysis()
        results.append(analyzer)
    
    # 汇总统计
    print(f"\n{'='*70}")
    print("汇总统计")
    print(f"{'='*70}")
    print(f"{'图名称':<30} {'节点':<6} {'边':<6} {'推荐色数':<10}")
    print(f"{'-'*70}")
    
    for analyzer in results:
        a = analyzer.analysis
        print(f"{a['name']:<30} {a['nodes']:<6} {a['edges']:<6} "
              f"{a['recommended_colors']:<10}")
    
    print(f"{'='*70}\n")
    
    return results


if __name__ == '__main__':
    analyze_all_graphs()


