# Grover Quantum Scheduler Simulator 使用指南

本文档介绍如何在新的机器上部署并运行 Grover 量子调度模拟器系统。

## 1. 系统架构

该系统包含三个核心组件：
1. **Kubernetes Scheduler Simulator** (Go/Docker)：提供 K8s 模拟环境和可视化界面 (localhost:3000)。
2. **Quantum Scheduling Service** (Python/FastAPI)：提供 Grover 图着色算法的 HTTP 服务 (localhost:8000)。
3. **Quantum Batch Operator** (Python)：监听 Pod 事件，自动触发量子批处理调度。

---

## 2. 环境准备

在开始之前，请确保你的机器安装了以下工具：

- **Docker & Docker Compose** (用于运行模拟器)
- **Python 3.9+** (推荐使用 Conda)
- **kubectl** (用于与模拟器交互)

### 创建 Python 环境

Operator 需要 Python 环境，建议使用 Conda：

```bash
conda create -n qiskit python=3.10
conda activate qiskit
# 只需要安装 Operator 的依赖（kubernetes 客户端库）
pip install kubernetes
```

> **注意**：量子服务（Qiskit、FastAPI 等）的依赖已经在 Docker 容器内安装，宿主机无需安装。

---

## 3. 快速启动（推荐）

我们提供了自动化测试脚本 `run_test.sh`，可以一键完成环境启动、Operator 运行和测试执行。

### 步骤：

1. **进入项目目录**：
   ```bash
   cd Grover-kube-scheduler-simulator
   ```

2. **运行脚本**：
   ```bash
   ./run_test.sh
   ```

3. **脚本会自动执行以下操作**：
   - 清理并重启所有 Docker 容器
   - 启动 Quantum Operator
   - 显示交互式菜单供你选择测试场景
   - 自动运行测试并展示节点优化结果
   - 退出时自动清理资源

---

## 4. 添加新的测试场景

如果你想测试新的图结构（Graph），只需按照以下步骤操作：

1. **准备 YAML 文件**：
   - 定义一组 Pod，并添加 `podAntiAffinity` 规则来描述图的边（冲突）。
   - 必须给所有 Pod 添加以下标签和注解：
     ```yaml
     metadata:
       labels:
         quantum-batch: "your-batch-name"  # 批次ID
       annotations:
         quantum-scheduler.io/batch-size: "N" # 该批次的总 Pod 数
     spec:
       schedulingGates:  # 必须添加 Gate，防止被默认调度器抢跑
       - name: "quantum-scheduler.io/computing"
     ```

2. **放入测试目录**：
   将 YAML 文件放入 `Grover-kube-scheduler-simulator/TEST_INPUT/` 目录。

3. **运行测试**：
   使用 `kubectl apply` 或修改 `run_test.sh` 将新场景加入菜单。
   
   **手动测试新场景：**
   （在运行 `./run_test.sh` 的同时，另开一个终端）
   ```bash
   kubectl --server=http://localhost:3131 apply -f TEST_INPUT/your_new_graph.yaml --validate=false
   ```
   Operator 会自动检测到新批次并开始处理。

---

## 5. 手动操作指南（进阶）

如果你需要更细粒度的控制，可以手动启动各组件。

### 步骤 1：启动模拟器和量子服务
```bash
docker-compose up -d
```
验证：访问 http://localhost:3000 查看 UI。

### 步骤 2：启动 Operator
```bash
conda activate qiskit
python operator/quantum_operator.py
```

### 步骤 3：提交测试任务
```bash
# 提交预置的测试用例
kubectl --server=http://localhost:3131 apply -f TEST_INPUT/test_path_p5.yaml --validate=false
```

### 步骤 4：验证结果
查看 Operator 日志，或者查询 Pod 状态：
```bash
kubectl --server=http://localhost:3131 get pods -o wide
```

---

## 6. 常见问题

1. **Operator 显示连接失败？**
   - 确保模拟器已启动，且 `localhost:3131` 可访问。

2. **Pod 一直处于 Pending / SchedulingGated 状态？**
   - 检查 Operator 是否在运行。
   - 检查量子服务容器日志：`docker logs quantum-service`。

3. **测试结果不符合预期？**
   - 检查 YAML 文件是否正确配置了 `podAntiAffinity`。
   - 检查是否遗漏了 `schedulingGates`（导致被默认调度器抢先调度）。

4. **第二次运行测试没反应？**
   - 先清理旧数据：`kubectl --server=http://localhost:3131 delete pods --all`。
