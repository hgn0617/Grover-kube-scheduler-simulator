#!/bin/bash
# Grover Quantum Scheduler Test Script - Modified for Batch Testing

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 全局变量
OPERATOR_PID=""
OPERATOR_LOG="/tmp/quantum_operator.log"
RESULTS_DIR="results"
REPORT_FILE="$RESULTS_DIR/benchmark_report.csv"

# 打印带颜色的消息
print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# 初始化结果目录和报告文件
init_results() {
    mkdir -p "$RESULTS_DIR"
    if [ ! -f "$REPORT_FILE" ]; then
        echo "Timestamp,TestName,BatchName,PodCount,UsedNodes,QuantumTime(s),TotalTime(s),PodDistribution,Status" > "$REPORT_FILE"
    fi
}

# 清理函数（在脚本退出时调用）
cleanup() {
    print_info "正在清理资源..."
    
    # 停止 Operator
    if [ -n "$OPERATOR_PID" ] && kill -0 $OPERATOR_PID 2>/dev/null; then
        print_info "停止 Quantum Operator (PID: $OPERATOR_PID)..."
        kill $OPERATOR_PID 2>/dev/null
        wait $OPERATOR_PID 2>/dev/null
        print_success "Operator 已停止"
    fi
    
    # 清理临时日志
    rm -f "$OPERATOR_LOG"
    
    print_success "清理完成"
}

# 注册退出时的清理函数
trap cleanup EXIT INT TERM

# 步骤 1: 停止并清理旧容器
stop_old_containers() {
    print_info "步骤 1/4: 停止旧容器..."
    docker-compose down -v 2>/dev/null
    print_success "旧容器已停止并清理"
}

# 步骤 2: 构建并启动新容器
start_containers() {
    print_info "步骤 2/4: 构建并启动容器..."
    docker-compose up -d --build
    
    if [ $? -ne 0 ]; then
        print_error "容器启动失败"
        exit 1
    fi
    
    print_success "容器启动成功"
    
    # 等待服务就绪
    print_info "等待服务就绪..."
    sleep 5
    
    # 检查量子服务健康状态
    for i in {1..10}; do
        if curl -s http://localhost:8000/health >/dev/null 2>&1; then
            print_success "量子服务已就绪"
            break
        fi
        if [ $i -eq 10 ]; then
            print_warning "量子服务健康检查超时，但继续执行..."
        fi
        sleep 2
    done
}

# 步骤 3: 启动 Operator
start_operator() {
    print_info "步骤 3/4: 启动 Quantum Operator..."
    
    # 检查 conda 环境
    if ! conda env list | grep -q "qiskit"; then
        print_error "未找到 qiskit conda 环境，请先创建：conda create -n qiskit python=3.10"
        exit 1
    fi
    
    # 后台启动 Operator，输出重定向到日志文件
    # 使用 -u 参数让 Python 使用 unbuffered 输出，确保日志实时写入
    conda run -n qiskit python -u operator/quantum_operator.py > "$OPERATOR_LOG" 2>&1 &
    OPERATOR_PID=$!
    
    # 等待 Operator 初始化
    sleep 3
    
    # 检查 Operator 是否成功启动
    if ! kill -0 $OPERATOR_PID 2>/dev/null; then
        print_error "Operator 启动失败，查看日志："
        tail -n 20 "$OPERATOR_LOG"
        exit 1
    fi
    
    print_success "Operator 已启动 (PID: $OPERATOR_PID)"
    print_info "Operator 日志文件: $OPERATOR_LOG"
}

# 步骤 4: 显示菜单并运行测试
run_test_menu() {
    print_info "步骤 4/4: 准备运行测试"
    init_results
    echo ""
    
    while true; do
        echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
        echo -e "${GREEN}   Grover 量子调度器测试菜单${NC}"
        echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
        echo ""
        
        # 动态扫描 TEST_INPUT 目录下的 yaml 文件
        local i=1
        local files=()
        
        if [ -d "TEST_INPUT" ]; then
            for file in TEST_INPUT/*.yaml; do
                [ -e "$file" ] || continue
                filename=$(basename "$file")
                files[$i]="$filename"
                echo "  $i) 运行测试: $filename"
                ((i++))
            done
        else
            print_warning "未找到 TEST_INPUT 目录！"
        fi
        
        echo ""
        echo "  batch) 批量运行前5个测试 (Benchmark)"
        echo "  all) 批量运行所有测试"
        echo "  v) 查看 Operator 实时日志"
        echo "  q) 退出测试"
        echo ""
        echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
        echo -n "请选择 [1-$((i-1)), batch, all, v, q]: "
        read choice
        
        case $choice in
            [vV])
                print_info "Operator 实时日志 (按 Ctrl+C 返回菜单)："
                tail -f "$OPERATOR_LOG"
                ;;
            [qQ0])
                print_info "退出测试..."
                break
                ;;
            "batch")
                print_info "开始批量运行测试 1-5..."
                for idx in {1..5}; do
                    if [ $idx -lt $i ]; then
                        process_test_selection $idx "${files[$idx]}" "true"
                    fi
                done
                print_success "批量测试完成！请查看结果文件: $REPORT_FILE"
                cat "$REPORT_FILE"
                echo -n "按 Enter 键继续..."
                read
                ;;
            "all")
                print_info "开始运行所有测试..."
                for ((idx=1; idx<i; idx++)); do
                    process_test_selection $idx "${files[$idx]}" "true"
                done
                print_success "所有测试完成！请查看结果文件: $REPORT_FILE"
                cat "$REPORT_FILE"
                echo -n "按 Enter 键继续..."
                read
                ;;
            *)
                if [[ "$choice" =~ ^[0-9]+$ ]] && [ "$choice" -ge 1 ] && [ "$choice" -lt "$i" ]; then
                    process_test_selection "$choice" "${files[$choice]}" "false"
                else
                    print_warning "无效选择，请重新输入"
                fi
                ;;
        esac
        
        echo ""
    done
}

# 运行单个测试
# 参数: yaml_file batch_name test_name pod_count expected_nodes auto_mode
run_test() {
    local yaml_file=$1
    local batch_name=$2
    local test_name=$3
    local pod_count=$4
    local expected_nodes=$5
    local auto_mode=$6 # 是否自动模式（不暂停）
    
    echo ""
    print_info "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    print_info "开始测试: $test_name (Batch: $batch_name)"
    print_info "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    local start_time=$(date +%s)
    
    # 清理环境
    print_info "清理旧的 Pods 和 Nodes..."
    kubectl --server=http://localhost:3131 delete pods --all --wait=false 2>/dev/null
    kubectl --server=http://localhost:3131 delete nodes --all --wait=false 2>/dev/null
    sleep 2
    
    # 清空 Operator 日志以便观察本次测试
    > "$OPERATOR_LOG"
    
    # 应用测试 YAML
    print_info "应用测试场景: TEST_INPUT/$yaml_file"
    kubectl --server=http://localhost:3131 apply -f TEST_INPUT/$yaml_file --validate=false
    
    if [ $? -ne 0 ]; then
        print_error "应用 YAML 失败"
        return 1
    fi
    
    print_success "测试场景已应用"
    print_info "等待 Operator 处理批次 '$batch_name'..."
    
    # 监控 Operator 日志，等待批次处理完成
    local timeout=4800  # 30分钟超时（某些复杂图的量子计算可能需要较长时间）
    local elapsed=0
    local completed=false
    local batch_collected=false
    
    while [ $elapsed -lt $timeout ]; do
        # 检查批次是否收集完成
        if [ "$batch_collected" = false ] && grep -qE "Batch '$batch_name'.*完整" "$OPERATOR_LOG" 2>/dev/null; then
            print_success "批次已收集完成，量子计算中..."
            batch_collected=true
        fi
        
        # 检查是否处理完成（使用更宽松的匹配）
        if grep -qE "Batch.*'$batch_name'.*处理完成" "$OPERATOR_LOG" 2>/dev/null || \
           grep -qE "✓.*$batch_name.*→" "$OPERATOR_LOG" 2>/dev/null || \
           grep -q "🎉" "$OPERATOR_LOG" 2>/dev/null; then
            completed=true
            break
        fi
        
        # 备用检测：检查是否所有 Pod 已经绑定到节点
        local scheduled_pods=$(kubectl --server=http://localhost:3131 get pods -o wide 2>/dev/null | \
                               awk 'NR>1 && $7!="<none>" {c++} END {print c+0}')
        if [ -n "$scheduled_pods" ] && [ "$scheduled_pods" -ge "$pod_count" ]; then
            print_success "检测到所有 Pod 已调度到节点"
            completed=true
            break
        fi
        
        sleep 2
        elapsed=$((elapsed + 2))
        echo -n "."
    done
    echo ""
    
    local end_time=$(date +%s)
    local total_duration=$((end_time - start_time))
    
    if [ "$completed" = false ]; then
        print_warning "处理超时，查看 Operator 日志："
        tail -n 30 "$OPERATOR_LOG"
        # 记录失败结果
        echo "$(date '+%Y-%m-%d %H:%M:%S'),$test_name,$batch_name,$pod_count,0,0,$total_duration,TIMEOUT,FAILED" >> "$REPORT_FILE"
        return 1
    fi
    
    print_success "量子计算完成！"
    
    # 等待 Pods 调度完成
    print_info "等待 Pods 调度到节点..."
    for t in {1..30}; do
        scheduled_now=$(kubectl --server=http://localhost:3131 get pods -o wide 2>/dev/null | \
                         awk 'NR>1 && $7!="<none>" {c++} END {print c+0}')
        if [ -n "$scheduled_now" ] && [ "$scheduled_now" -ge "$pod_count" ]; then
            break
        fi
        sleep 1
    done
    
    # 显示结果
    echo ""
    print_info "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    print_success "测试结果: $test_name"
    print_info "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    kubectl --server=http://localhost:3131 get pods -o wide
    
    # 统计节点使用情况
    local used_nodes=$(kubectl --server=http://localhost:3131 get pods -o wide 2>/dev/null | \
                       awk 'NR>1 && $7!="<none>" {print $7}' | sort -u | wc -l | tr -d ' ')
    
    # 收集 Pod 分布情况 (Node:Count)
    local pod_dist=$(kubectl --server=http://localhost:3131 get pods -o wide 2>/dev/null | \
                     awk 'NR>1 && $7!="<none>" {print $7}' | sort | uniq -c | awk '{print $2 ":" $1}' | paste -sd "|" -)
    
    # 尝试从 Operator 日志提取量子计算时间
    local quantum_time=$(grep -E "耗时|time:|took" "$OPERATOR_LOG" | tail -n 1 | grep -oE "[0-9]+\.[0-9]+" | head -n 1)
    if [ -z "$quantum_time" ]; then quantum_time="0"; fi
    
    echo ""
    print_info "性能统计："
    echo "  - 总耗时: ${total_duration}s"
    echo "  - 量子计算耗时(估): ${quantum_time}s"
    echo "  - 使用节点数: $used_nodes"
    echo "  - Pod 分布: $pod_dist"
    
    # 记录到 CSV（若未调度则记为 FAILED）
    local status="SUCCESS"
    if [ -z "$pod_dist" ] || [ "$used_nodes" -eq 0 ]; then
        status="FAILED"
        print_warning "未检测到任何 Pod 被调度到节点，此轮结果将标记为 FAILED"
    fi
    echo "$(date '+%Y-%m-%d %H:%M:%S'),$test_name,$batch_name,$pod_count,$used_nodes,$quantum_time,$total_duration,$pod_dist,$status" >> "$REPORT_FILE"
    print_success "结果已保存至: $REPORT_FILE"
    
    # 只有当 expected_nodes 是有效数字时才进行比较
    if [[ "$expected_nodes" =~ ^[0-9]+$ ]]; then
        if [ "$used_nodes" -eq "$expected_nodes" ]; then
            print_success "✨ 测试通过！节点优化符合预期"
        else
            print_warning "节点使用数量与预期不符 (预期: $expected_nodes, 实际: $used_nodes)"
        fi
    fi
    
    # 显示 Operator 关键日志
    echo ""
    print_info "Operator 关键日志摘要："
    grep -E "(Batch|量子|颜色|recommended-node|耗时|time)" "$OPERATOR_LOG" | tail -n 10
    
    echo ""
    print_info "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    if [ "$auto_mode" != "true" ]; then
        echo -n "按 Enter 键返回菜单..."
        read
    else
        # 自动模式下稍微等待一下，方便看一眼结果
        sleep 3
    fi
}

process_test_selection() {
    local idx=$1
    local selected_file=$2
    local auto_mode=$3
    
    # 提取 batch_name
    local batch_name=$(echo "$selected_file" | sed 's/test_//' | sed 's/\.yaml//')
    
    # 尝试从文件内容读取 batch-size
    local pod_count=$(grep "quantum-scheduler.io/batch-size" "TEST_INPUT/$selected_file" | head -n 1 | awk -F'"' '{print $2}')
    if [ -z "$pod_count" ]; then pod_count=5; fi
    
    run_test "$selected_file" "$batch_name" "$selected_file" "$pod_count" "Unknown" "$auto_mode"
}

# 主函数
main() {
    echo -e "${GREEN}"
    echo "╔════════════════════════════════════════════════════════╗"
    echo "║   Grover Quantum Scheduler - 自动化测试脚本           ║"
    echo "╚════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
    
    # 检查是否在正确的目录
    if [ ! -f "compose.yml" ] || [ ! -d "operator" ]; then
        print_error "请在 Grover-kube-scheduler-simulator 目录下运行此脚本"
        exit 1
    fi
    
    # 执行各个步骤
    stop_old_containers
    start_containers
    start_operator
    run_test_menu
    
    print_success "感谢使用 Grover Quantum Scheduler 测试系统！"
}

# 运行主函数
main
