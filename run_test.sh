#!/bin/bash
# Grover Quantum Scheduler Test Script - Modified for Batch Testing

# ──────────────────────────────────────────────────────────────────────────────
# 量子服务部署方式（可交互选择，也可通过环境变量预设）：
#
#   本地 Docker:  QUANTUM_MODE=local   ./run_test.sh
#   远程直连:     QUANTUM_MODE=remote QUANTUM_SERVICE_URL=http://SERVER_IP:8000 ./run_test.sh
#   远程隧道:     先 ssh -N -L 8000:127.0.0.1:8000 user@server
#                 再 QUANTUM_MODE=remote QUANTUM_SERVICE_URL=http://127.0.0.1:8000 ./run_test.sh
#
# 若不预设 QUANTUM_MODE，脚本启动时会交互询问。
# ──────────────────────────────────────────────────────────────────────────────

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

# 量子服务 URL。会在 choose_quantum_service_mode 里根据模式最终设定；这里仅提供预设。
#   本地模式默认:  http://localhost:8000
#   远程模式默认:  http://192.168.223.77:8000  （也可在环境变量里预设 QUANTUM_SERVICE_URL）
QUANTUM_SERVICE_URL_REMOTE_DEFAULT="${QUANTUM_SERVICE_URL:-http://192.168.223.77:8000}"
QUANTUM_SERVICE_URL_LOCAL_DEFAULT="http://localhost:8000"

# Per-k attempt budget T for the descent (BBHT-style attempts per budget).
# Default is conservative for runtime; increase if you see frequent false negatives at k-1.
QUANTUM_ATTEMPT_BUDGET="${QUANTUM_ATTEMPT_BUDGET:-10}"
export QUANTUM_ATTEMPT_BUDGET

# Stop any previously started Operator processes.
# Note: conda-run may orphan the child python process if the parent is killed,
# so we explicitly kill by command pattern to avoid duplicate scheduling calls.
stop_operator_processes() {
    pkill -f "operator/quantum_operator.py" 2>/dev/null || true
    pkill -f "conda run -n qiskit.*operator/quantum_operator.py" 2>/dev/null || true
}

# 检查 Operator 是否仍然存活，若已退出则自动重启。
# 用在每个 case 收尾，避免静默卡死导致后续 case 全部拿不到 metrics。
ensure_operator_alive() {
    if [ -z "$OPERATOR_PID" ] || ! kill -0 "$OPERATOR_PID" 2>/dev/null; then
        print_warning "检测到 Operator 已退出 (PID=${OPERATOR_PID:-n/a})，最近日志："
        tail -n 15 "$OPERATOR_LOG" 2>/dev/null || true
        print_info "自动重启 Operator..."
        start_operator
    fi
}

# Compose wrapper:
# - USE_DOCKER_QUANTUM_SERVICE=1: bring up local quantum-service via compose.quantum.yml
# - USE_DOCKER_QUANTUM_SERVICE=0: do NOT start quantum-service locally; use QUANTUM_SERVICE_URL instead
# 默认留空，由 choose_quantum_service_mode 设定。
USE_DOCKER_QUANTUM_SERVICE="${USE_DOCKER_QUANTUM_SERVICE:-}"
dc() {
    if [ "$USE_DOCKER_QUANTUM_SERVICE" = "1" ] && [ -f "compose.quantum.yml" ]; then
        docker-compose -f compose.yml -f compose.quantum.yml "$@"
    else
        docker-compose -f compose.yml "$@"
    fi
}

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
    local header="Timestamp,TestName,BatchName,PodCount,QuantumUsedNodes,GreedyUsedNodes,DSATURUsedNodes,QuantumRPC(ms),ExtractConflicts(ms),AnnotateApply(ms),ClearGates(ms),OperatorTotal(ms),ConflictEdges,KUpper,KStart,KFound,Attempts,OracleCalls,TotalTime(s),PodDistribution,Status"
    if [ ! -f "$REPORT_FILE" ]; then
        echo "$header" > "$REPORT_FILE"
        return
    fi

    local first_line
    first_line=$(head -n 1 "$REPORT_FILE" 2>/dev/null || echo "")
    if [ "$first_line" = "$header" ]; then
        return
    fi

    local backup="$REPORT_FILE.bak_$(date '+%Y%m%d_%H%M%S')"
    cp "$REPORT_FILE" "$backup"
    echo "$header" > "$REPORT_FILE"

    # Best-effort migration from older report layouts.
    if [ "$first_line" = "Timestamp,TestName,BatchName,PodCount,QuantumUsedNodes,GreedyUsedNodes,DSATURUsedNodes,QuantumTime(s),TotalTime(s),PodDistribution,Status" ]; then
        tail -n +2 "$backup" | awk -F',' 'BEGIN{OFS=","} NF>=11 {print $1,$2,$3,$4,$5,$6,$7,"","","","","","","","","","","",$9,$10,$11}' >> "$REPORT_FILE"
    elif [ "$first_line" = "Timestamp,TestName,BatchName,PodCount,UsedNodes,QuantumTime(s),TotalTime(s),PodDistribution,Status" ]; then
        tail -n +2 "$backup" | awk -F',' 'BEGIN{OFS=","} NF>=9 {print $1,$2,$3,$4,$5,"","", "", "", "", "", "", "", "", "", "", "", "", $7,$8,$9}' >> "$REPORT_FILE"
    else
        # Unknown header; keep old rows as-is (will be shorter).
        tail -n +2 "$backup" >> "$REPORT_FILE"
    fi
}

# Ensure the report file ends with a newline before appending.
# This prevents two CSV rows from being concatenated when the file was previously
# saved without a trailing newline.
ensure_report_trailing_newline() {
    if [ ! -f "$REPORT_FILE" ] || [ ! -s "$REPORT_FILE" ]; then
        return
    fi

    local last_char
    last_char=$(tail -c 1 "$REPORT_FILE" 2>/dev/null || true)
    if [ -n "$last_char" ]; then
        echo "" >> "$REPORT_FILE"
    fi
}

extract_operator_metrics() {
    local log_file=$1
    local start_line=$2
    local batch_name=$3
    python3 - "$log_file" "$start_line" "$batch_name" <<'PY'
import json
import sys

path = sys.argv[1]
start_line = int(sys.argv[2])
batch_name = sys.argv[3]
fields = [
    "quantum_rpc_ms",
    "extract_conflicts_ms",
    "annotate_apply_ms",
    "clear_gates_ms",
    "operator_total_ms",
    "conflict_edges",
    "k_upper",
    "k_start",
    "k_found",
    "attempts",
    "oracle_calls",
]
record = {}
marker = "[Operator][Metrics]"

try:
    with open(path, encoding="utf-8", errors="ignore") as handle:
        for lineno, line in enumerate(handle, start=1):
            if lineno < start_line or marker not in line:
                continue
            payload = line.split(marker, 1)[1].strip()
            try:
                obj = json.loads(payload)
            except Exception:
                continue
            if obj.get("batch_id") == batch_name:
                record = obj
except FileNotFoundError:
    pass

values = []
for field in fields:
    value = record.get(field, "")
    if isinstance(value, float):
        values.append(f"{value:.3f}")
    elif value is None:
        values.append("")
    else:
        values.append(str(value))
print("\t".join(values))
PY
}

expand_selection_expression() {
    local expr=$1
    local max_index=$2
    python3 - "$expr" "$max_index" <<'PY'
import sys

expr = sys.argv[1].replace(" ", "")
max_index = int(sys.argv[2])

if not expr:
    raise SystemExit(1)

seen = set()
ordered = []

for token in expr.split(","):
    if not token:
        raise SystemExit(1)
    if "-" in token:
        parts = token.split("-")
        if len(parts) != 2 or not parts[0].isdigit() or not parts[1].isdigit():
            raise SystemExit(1)
        start = int(parts[0])
        end = int(parts[1])
        if start > end:
            raise SystemExit(1)
        values = range(start, end + 1)
    else:
        if not token.isdigit():
            raise SystemExit(1)
        values = [int(token)]

    for value in values:
        if value < 1 or value > max_index:
            raise SystemExit(1)
        if value not in seen:
            seen.add(value)
            ordered.append(value)

for value in ordered:
    print(value)
PY
}

# 清理函数（在脚本退出时调用）
cleanup() {
    print_info "正在清理资源..."
    
    # 停止 Operator
    stop_operator_processes
    
    # 清理临时日志
    rm -f "$OPERATOR_LOG"
    
    print_success "清理完成"
    exit 0  # 强制退出
}

# 注册退出时的清理函数
trap 'cleanup; exit 0' INT TERM
trap cleanup EXIT

# 步骤 0: 选择量子服务部署方式（本地 Docker / 远程服务器）
#   支持预设：QUANTUM_MODE=local 或 QUANTUM_MODE=remote；此时不会交互询问。
#   远程模式下也可通过 QUANTUM_SERVICE_URL 预设 URL，否则使用默认远程地址。
choose_quantum_service_mode() {
    local mode="${QUANTUM_MODE:-}"

    if [ -z "$mode" ]; then
        echo ""
        echo "────────────────────────────────────────────────────"
        echo "  步骤 0/4: 选择量子服务部署方式"
        echo "────────────────────────────────────────────────────"
        echo "  1) 本地 Docker  (通过 compose.quantum.yml 在本机启动 quantum-service)"
        echo "  2) 远程服务器  (连接已部署的远端 quantum-service，省本机内存/GPU)"
        echo ""
        printf "请选择 [1/2] (默认 2): "
        local choice
        read -r choice
        choice="${choice:-2}"
        case "$choice" in
            1) mode="local" ;;
            2) mode="remote" ;;
            *) print_error "无效选项: $choice"; exit 1 ;;
        esac
    fi

    case "$mode" in
        local)
            USE_DOCKER_QUANTUM_SERVICE=1
            QUANTUM_SERVICE_URL="${QUANTUM_SERVICE_URL_LOCAL_DEFAULT}"
            print_success "量子服务模式: 本地 Docker  → ${QUANTUM_SERVICE_URL}"
            ;;
        remote)
            USE_DOCKER_QUANTUM_SERVICE=0
            # 远程模式下若未交互设过 URL，使用默认；若交互进来，允许确认或改写
            local default_url="${QUANTUM_SERVICE_URL_REMOTE_DEFAULT}"
            if [ -z "${QUANTUM_MODE:-}" ]; then
                printf "  远程量子服务 URL (回车使用默认 %s): " "$default_url"
                local url_input
                read -r url_input
                QUANTUM_SERVICE_URL="${url_input:-$default_url}"
            else
                QUANTUM_SERVICE_URL="${QUANTUM_SERVICE_URL:-$default_url}"
            fi
            QUANTUM_SERVICE_URL="${QUANTUM_SERVICE_URL%/}"
            print_success "量子服务模式: 远程服务器 → ${QUANTUM_SERVICE_URL}"
            ;;
        *)
            print_error "未知的 QUANTUM_MODE: $mode (应为 local 或 remote)"
            exit 1
            ;;
    esac

    export USE_DOCKER_QUANTUM_SERVICE
    export QUANTUM_SERVICE_URL
}

# 步骤 1: 停止并清理旧容器
stop_old_containers() {
    print_info "步骤 1/4: 停止旧容器..."
    dc down -v 2>/dev/null
    print_success "旧容器已停止并清理"
}

# 步骤 2: 构建并启动新容器
start_containers() {
    print_info "步骤 2/4: 构建并启动容器..."
    dc up -d --build
    
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
        if curl -s "${QUANTUM_SERVICE_URL}/health" >/dev/null 2>&1; then
            print_success "量子服务已就绪"
            break
        fi
        if [ $i -eq 10 ]; then
            print_warning "量子服务健康检查超时（URL: ${QUANTUM_SERVICE_URL}；若远程服务尚未启动可忽略）..."
        fi
        sleep 2
    done
}

# 步骤 3: 启动 Operator
start_operator() {
    print_info "步骤 3/4: 启动 Quantum Operator..."

    # Avoid multiple Operators running concurrently (will cause repeated /batch_schedule calls).
    stop_operator_processes
    
    # 检查 conda 环境
    if ! conda env list | grep -q "qiskit"; then
        print_error "未找到 qiskit conda 环境，请先创建：conda create -n qiskit python=3.10"
        exit 1
    fi
    
    # 后台启动 Operator，输出重定向到日志文件
    # conda run 默认会捕获输出，导致日志无法实时写入；这里强制 live-stream
    # 使用 -u 参数让 Python 使用 unbuffered 输出，确保日志实时写入
    conda run -n qiskit --no-capture-output python -u operator/quantum_operator.py > "$OPERATOR_LOG" 2>&1 &
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
        echo "  hard) 批量运行自定义困难30图 (31-60)"
        echo "  dsatur) 批量运行 DSATUR 出错5图 (56-60)"
        echo "  all) 批量运行所有测试"
        echo -e "  ${GREEN}3x) 自动跑 1-50 三轮，每轮独立 CSV（用于取均值）${NC}"
        echo -e "  ${GREEN}5x) 自动跑 1-50 五轮，每轮独立 CSV${NC}"
        echo -e "  ${GREEN}10x) 自动跑 1-50 十轮，每轮独立 CSV（run1-run10）${NC}"
        echo -e "  ${GREEN}sweep3) r_max 参数扫描: {5,10,20} 三档 × 3轮（按批次选择）${NC}"
        echo -e "  ${GREEN}sweep5) r_max 参数扫描: {5,10,20} 三档 × 5轮（按批次选择）${NC}"
        echo "  例如: 46,47,48,49 或 46-49"
        echo "  v) 查看 Operator 实时日志"
        echo "  q) 退出测试"
        echo ""
        echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
        echo -n "请选择 [1-$((i-1)), batch, 3x, 5x, 10x, sweep3, sweep5, hard, dsatur, all, v, q]: "
        read choice || break  # 如果read被中断，退出循环
        
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
            "3x"|"5x"|"10x")
                # ── 自动跑 case 1-50 多轮，每轮独立 CSV ──────────────────
                local total_runs=3
                [ "$choice" = "5x" ] && total_runs=5
                [ "$choice" = "10x" ] && total_runs=10
                local max_case=50
                if [ $i -le $max_case ]; then max_case=$((i-1)); fi
                local csv_header
                csv_header=$(head -n 1 "$REPORT_FILE" 2>/dev/null)
                if [ -z "$csv_header" ]; then
                    csv_header="Timestamp,TestName,BatchName,PodCount,QuantumUsedNodes,GreedyUsedNodes,DSATURUsedNodes,QuantumRPC(ms),ExtractConflicts(ms),AnnotateApply(ms),ClearGates(ms),OperatorTotal(ms),ConflictEdges,KUpper,KStart,KFound,Attempts,OracleCalls,TotalTime(s),PodDistribution,Status"
                fi

                for run_no in $(seq 1 $total_runs); do
                    echo ""
                    echo -e "${GREEN}═══════════════════════════════════════════${NC}"
                    echo -e "${GREEN}   ${choice} 模式 — RUN $run_no / $total_runs  (case 1-$max_case)${NC}"
                    echo -e "${GREEN}═══════════════════════════════════════════${NC}"

                    # 重置 report CSV（仅保留 header；不再生成 .bak）
                    echo "$csv_header" > "$REPORT_FILE"

                    for idx in $(seq 1 $max_case); do
                        if [ -n "${files[$idx]:-}" ]; then
                            process_test_selection $idx "${files[$idx]}" "true"
                        fi
                    done

                    # 归档本轮结果
                    local dest="$RESULTS_DIR/benchmark_report_run${run_no}.csv"
                    cp "$REPORT_FILE" "$dest"
                    local row_count
                    row_count=$(tail -n +2 "$dest" | wc -l | tr -d ' ')
                    print_success "Run $run_no / $total_runs 完成: $row_count 行 → $dest"
                done

                echo ""
                print_success "${total_runs} 轮全部完成！"
                echo -e "${BLUE}合并汇总请运行:${NC}"
                echo -e "  conda run -n qiskit python ../scripts/merge_benchmark_runs.py"
                echo ""
                echo -n "按 Enter 键继续..."
                read
                ;;
            "sweep3"|"sweep5")
                # ── r_max 参数扫描: {5,10,20} 三档 × N轮，按批次选择 ────────────────────────
                local all_rmax=(5 10 20)
                local runs_per_rmax=3
                [ "$choice" = "sweep5" ] && runs_per_rmax=5
                local sweep_label="$choice"
                local max_case=50
                if [ $i -le $max_case ]; then max_case=$((i-1)); fi

                # 批次选择
                echo ""
                echo -e "${BLUE}请选择要跑的 r_max 批次:${NC}"
                echo -e "  ${GREEN}1) r_max=5${NC}   (${runs_per_rmax} 轮 × ${max_case} case)"
                echo -e "  ${GREEN}2) r_max=10${NC}  (${runs_per_rmax} 轮 × ${max_case} case)"
                echo -e "  ${GREEN}3) r_max=20${NC}  (${runs_per_rmax} 轮 × ${max_case} case)"
                echo -e "${BLUE}多批用逗号分隔 (例: 1,3)；回车 = 全部三批${NC}"
                echo -n "请选择: "
                read sweep_choice || { print_error "输入中断"; continue; }

                # 解析批次选择
                local rmax_values=()
                if [ -z "$sweep_choice" ]; then
                    rmax_values=("${all_rmax[@]}")
                else
                    local IFS_BACKUP="$IFS"
                    IFS=',' read -ra choice_tokens <<< "$sweep_choice"
                    IFS="$IFS_BACKUP"
                    local invalid=false
                    for tok in "${choice_tokens[@]}"; do
                        tok="$(echo "$tok" | tr -d '[:space:]')"
                        case "$tok" in
                            1) rmax_values+=(5) ;;
                            2) rmax_values+=(10) ;;
                            3) rmax_values+=(20) ;;
                            *)  print_error "无效选项: '$tok' (只接受 1/2/3)"
                                invalid=true ;;
                        esac
                    done
                    if [ "$invalid" = true ] || [ ${#rmax_values[@]} -eq 0 ]; then
                        echo -n "按 Enter 继续..."
                        read
                        continue
                    fi
                fi

                local csv_header
                csv_header=$(head -n 1 "$REPORT_FILE" 2>/dev/null)
                if [ -z "$csv_header" ]; then
                    csv_header="Timestamp,TestName,BatchName,PodCount,QuantumUsedNodes,GreedyUsedNodes,DSATURUsedNodes,QuantumRPC(ms),ExtractConflicts(ms),AnnotateApply(ms),ClearGates(ms),OperatorTotal(ms),ConflictEdges,KUpper,KStart,KFound,Attempts,OracleCalls,TotalTime(s),PodDistribution,Status"
                fi

                local total_groups=${#rmax_values[@]}
                local total_planned=$((total_groups * runs_per_rmax))
                echo ""
                echo -e "${BLUE}扫描计划: r_max ∈ {${rmax_values[*]}}，每个 r_max 跑 ${runs_per_rmax} 轮 = ${total_planned} 轮共计${NC}"
                echo -e "${BLUE}CSV 命名: benchmark_report_r{rmax}_run{i}.csv${NC}"
                echo -e "${YELLOW}⚠️  每个 r_max 切换时会重启 Operator 传入新的 QUANTUM_MAX_GROVER_ITERATIONS${NC}"
                echo ""

                local group_idx=0
                for rmax in "${rmax_values[@]}"; do
                    group_idx=$((group_idx + 1))
                    echo ""
                    echo -e "${GREEN}═══════════════════════════════════════════════${NC}"
                    echo -e "${GREEN}   ${sweep_label} group ${group_idx}/${total_groups} — r_max=${rmax}${NC}"
                    echo -e "${GREEN}═══════════════════════════════════════════════${NC}"

                    # 重启 Operator，传入新的 r_max
                    export QUANTUM_MAX_GROVER_ITERATIONS=$rmax
                    print_info "重启 Operator，QUANTUM_MAX_GROVER_ITERATIONS=${rmax}"
                    start_operator

                    for run_no in $(seq 1 $runs_per_rmax); do
                        echo ""
                        echo -e "${BLUE}━━━ r_max=${rmax} · run ${run_no}/${runs_per_rmax} ━━━${NC}"

                        # 重置 report CSV（不再生成 .bak）
                        echo "$csv_header" > "$REPORT_FILE"

                        for idx in $(seq 1 $max_case); do
                            if [ -n "${files[$idx]:-}" ]; then
                                process_test_selection $idx "${files[$idx]}" "true"
                            fi
                        done

                        # 归档
                        local rmax_tag
                        rmax_tag=$(printf "r%02d" "$rmax")
                        local dest="$RESULTS_DIR/benchmark_report_${rmax_tag}_run${run_no}.csv"
                        cp "$REPORT_FILE" "$dest"
                        local row_count
                        row_count=$(tail -n +2 "$dest" | wc -l | tr -d ' ')
                        print_success "r_max=${rmax} run ${run_no}/${runs_per_rmax}: ${row_count} 行 → $dest"
                    done
                done

                # 恢复默认 r_max。如果用户没有设置，则清除环境变量让 operator 回到库默认（5）
                unset QUANTUM_MAX_GROVER_ITERATIONS
                print_info "已恢复 QUANTUM_MAX_GROVER_ITERATIONS 为库默认"
                start_operator

                echo ""
                print_success "${sweep_label} 完成！共 ${total_planned} 轮结果已写入 (r_max ∈ {${rmax_values[*]}})"
                echo -e "${BLUE}合并汇总请运行:${NC}"
                echo -e "  conda run -n qiskit python ../scripts/merge_sweep_runs.py"
                echo ""
                echo -n "按 Enter 键继续..."
                read
                ;;
            "hard")
                if [ ! -f "results/custom_test_suite_summary.json" ]; then
                    print_error "未找到 results/custom_test_suite_summary.json，请先运行: python generate_custom_test_suite.py"
                    echo -n "按 Enter 键继续..."
                    read
                else
                    print_info "开始运行自定义困难测试集（30个图）..."
                    local custom_list
                    custom_list=$(python3 - <<'PY'
import json
import re
from pathlib import Path

p = Path("results/custom_test_suite_summary.json")
rows = json.loads(p.read_text(encoding="utf-8"))

def key(r):
    m = re.match(r"(\\d+)_", r.get("case", ""))
    return int(m.group(1)) if m else 10**9

for r in sorted(rows, key=key):
    case = r.get("case")
    if case:
        print(f"{case}.yaml")
PY
)
                    while IFS= read -r f; do
                        [ -z "$f" ] && continue
                        if [ -f "TEST_INPUT/$f" ]; then
                            process_test_selection 0 "$f" "true"
                        else
                            print_warning "缺少测试文件: TEST_INPUT/$f (跳过)"
                        fi
                    done <<< "$custom_list"
                    print_success "自定义测试集完成！请查看结果文件: $REPORT_FILE"
                    cat "$REPORT_FILE"
                    echo -n "按 Enter 键继续..."
                    read
                fi
                ;;
            "dsatur")
                if [ ! -f "results/custom_test_suite_summary.json" ]; then
                    print_error "未找到 results/custom_test_suite_summary.json，请先运行: python generate_custom_test_suite.py"
                    echo -n "按 Enter 键继续..."
                    read
                else
                    print_info "开始运行 DSATUR 出错样例（56-60）..."
                    local dsatur_list
                    dsatur_list=$(python3 - <<'PY'
import json
import re
from pathlib import Path

p = Path("results/custom_test_suite_summary.json")
rows = json.loads(p.read_text(encoding="utf-8"))

def case_no(case: str) -> int:
    m = re.match(r"(\\d+)_", case or "")
    return int(m.group(1)) if m else -1

for r in sorted(rows, key=lambda x: case_no(x.get("case", ""))):
    case = r.get("case") or ""
    no = case_no(case)
    if 56 <= no <= 60:
        print(f"{case}.yaml")
PY
)
                    # Fallback: if summary is stale, match YAMLs directly.
                    if [ -z "$dsatur_list" ]; then
                        print_warning "未从 summary 中找到 56-60（可能未重新生成套件），尝试从 TEST_INPUT 直接匹配..."
                        dsatur_list=$(ls -1 TEST_INPUT/5[6-9]_dsatur_fail_*.yaml TEST_INPUT/60_dsatur_fail_*.yaml 2>/dev/null | xargs -n 1 basename 2>/dev/null || true)
                    fi

                    if [ -z "$dsatur_list" ]; then
                        print_error "未找到 DSATUR 出错样例（56-60）的 YAML。请先运行: python generate_custom_test_suite.py"
                        echo -n "按 Enter 键继续..."
                        read
                        continue
                    fi

                    # Only print new rows for this run (the report file is append-only).
                    local report_start_line
                    report_start_line=$(wc -l < "$REPORT_FILE" 2>/dev/null || echo 0)
                    while IFS= read -r f; do
                        [ -z "$f" ] && continue
                        if [ -f "TEST_INPUT/$f" ]; then
                            process_test_selection 0 "$f" "true"
                        else
                            print_warning "缺少测试文件: TEST_INPUT/$f (跳过)"
                        fi
                    done <<< "$dsatur_list"
                    print_success "DSATUR 出错样例完成！请查看结果文件: $REPORT_FILE"
                    tail -n +$((report_start_line + 1)) "$REPORT_FILE" 2>/dev/null || true
                    echo -n "按 Enter 键继续..."
                    read
                fi
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
                elif [[ "$choice" == *","* || "$choice" == *"-"* ]]; then
                    local selected_indices
                    selected_indices=$(expand_selection_expression "$choice" $((i-1)))
                    if [ $? -ne 0 ] || [ -z "$selected_indices" ]; then
                        print_warning "无效批量选择，请使用类似 46,47,48,49 或 46-49"
                        continue
                    fi

                    print_info "开始运行自定义批量测试: $choice"
                    while IFS= read -r idx; do
                        [ -z "$idx" ] && continue
                        process_test_selection "$idx" "${files[$idx]}" "true"
                    done <<< "$selected_indices"
                    print_success "自定义批量测试完成！请查看结果文件: $REPORT_FILE"
                    tail -n 20 "$REPORT_FILE" 2>/dev/null || true
                    echo -n "按 Enter 键继续..."
                    read
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

    # 经典算法基线（用于结果对比，不参与本轮调度）
    local k_greedy="NA"
    local k_dsatur="NA"
    local json_graph="../input/${batch_name}.json"
    if [ -f "$json_graph" ] && [ -f "classical_baselines.py" ]; then
        read -r k_greedy k_dsatur <<< "$(python3 classical_baselines.py --json "$json_graph" 2>/dev/null || echo "NA NA")"
    fi
    print_info "经典对比（节点数/k）：Greedy(LF)=${k_greedy}, DSATUR=${k_dsatur}"
    
    # ── 允许最多 3 次尝试：首次 + 2 次重试（metrics 为空时重启 Operator 重跑）──
    local max_attempts=3
    local attempt=1
    local quantum_rpc_ms="" extract_conflicts_ms="" annotate_apply_ms="" clear_gates_ms="" operator_total_ms="" conflict_edges="" k_upper="" k_start="" k_found="" attempts="" oracle_calls=""
    local used_nodes=0 pod_dist=""
    local completed=false
    local op_log_start_line=0
    local timeout=7200  # 2小时超时
    
    while [ $attempt -le $max_attempts ]; do
        if [ $attempt -gt 1 ]; then
            print_warning "第 $((attempt-1)) 次未拿到 batch '$batch_name' 的 [Operator][Metrics]；重启 Operator 并重跑 (尝试 $attempt / $max_attempts)"
            stop_operator_processes
            start_operator
        fi

        # 清理环境（强制删除 + 显式等清空，避免名字冲突让 apply 退化成 PATCH 老 pod）
        print_info "清理旧的 Pods 和 Nodes..."
        kubectl --server=http://localhost:3131 delete pods --all --grace-period=0 --force --wait=false 2>/dev/null >/dev/null
        kubectl --server=http://localhost:3131 delete nodes --all --grace-period=0 --force --wait=false 2>/dev/null >/dev/null

        # 轮询确认 etcd 已清空；测试场景里这步通常 <1s，封顶 30s
        local cleanup_waited=0
        local pod_residue=0
        local node_residue=0
        for _ in $(seq 1 60); do
            pod_residue=$(kubectl --server=http://localhost:3131 get pods --no-headers 2>/dev/null | wc -l | tr -d ' ')
            node_residue=$(kubectl --server=http://localhost:3131 get nodes --no-headers 2>/dev/null | wc -l | tr -d ' ')
            [ -z "$pod_residue" ] && pod_residue=0
            [ -z "$node_residue" ] && node_residue=0
            if [ "$pod_residue" -eq 0 ] && [ "$node_residue" -eq 0 ]; then
                break
            fi
            sleep 0.5
            cleanup_waited=$((cleanup_waited + 1))
        done
        if [ "$pod_residue" -ne 0 ] || [ "$node_residue" -ne 0 ]; then
            print_warning "清理未完全（残留 pods=$pod_residue, nodes=$node_residue），继续 apply（可能导致首阶段等待变长）"
        elif [ $cleanup_waited -gt 4 ]; then
            print_info "清理完成（耗时约 $((cleanup_waited / 2)) 秒）"
        fi
        
        # 记录 Operator 日志起始行号（不要在 Operator 运行时 truncate，否则会产生 NUL/空洞导致 grep 识别为 binary）
        touch "$OPERATOR_LOG"
        op_log_start_line=$(wc -l < "$OPERATOR_LOG" 2>/dev/null | tr -d ' ')
        if [ -z "$op_log_start_line" ]; then op_log_start_line=0; fi
        
        # 应用测试 YAML
        if [ $attempt -eq 1 ]; then
            print_info "应用测试场景: TEST_INPUT/$yaml_file"
        else
            print_info "应用测试场景: TEST_INPUT/$yaml_file（尝试 $attempt / $max_attempts）"
        fi
        kubectl --server=http://localhost:3131 apply -f TEST_INPUT/$yaml_file --validate=false
        
        if [ $? -ne 0 ]; then
            print_error "应用 YAML 失败"
            return 1
        fi
        
        print_success "测试场景已应用"
        print_info "等待 Operator 处理批次 '$batch_name'..."
        
        # 监控 Operator 日志，等待批次处理完成
        local elapsed=0
        local batch_collected=false
        completed=false
        
        while [ $elapsed -lt $timeout ]; do
            # 检查批次是否收集完成
            if [ "$batch_collected" = false ] && tail -n +$((op_log_start_line + 1)) "$OPERATOR_LOG" | grep -aqE "Batch '$batch_name'.*完整" 2>/dev/null; then
                print_success "批次已收集完成，量子计算中..."
                batch_collected=true
            fi

            # 严格完成判据：日志里必须出现本 batch 的 [Operator][Metrics] JSON 行
            # （operator 处理完才会打这一行，同时带 batch_id=当前批次，避免被其他 batch 的 🎉 误触发）
            if tail -n +$((op_log_start_line + 1)) "$OPERATOR_LOG" \
                 | grep -aF "[Operator][Metrics]" \
                 | grep -qF "\"batch_id\": \"$batch_name\"" 2>/dev/null; then
                completed=true
                break
            fi

            sleep 2
            elapsed=$((elapsed + 2))
            echo -n "."
        done
        echo ""
        
        # 超时：不再重试，走 TIMEOUT 分支
        if [ "$completed" = false ]; then
            break
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
        
        # 统计节点使用情况
        used_nodes=$(kubectl --server=http://localhost:3131 get pods -o wide 2>/dev/null | \
                     awk 'NR>1 && $7!="<none>" {print $7}' | sort -u | wc -l | tr -d ' ')
        pod_dist=$(kubectl --server=http://localhost:3131 get pods -o wide 2>/dev/null | \
                   awk 'NR>1 && $7!="<none>" {print $7}' | sort | uniq -c | awk '{print $2 ":" $1}' | paste -sd "|" -)
        
        # 提取结构化 Operator 阶段指标
        local operator_metrics
        operator_metrics=$(extract_operator_metrics "$OPERATOR_LOG" $((op_log_start_line + 1)) "$batch_name")
        IFS=$'\t' read -r quantum_rpc_ms extract_conflicts_ms annotate_apply_ms clear_gates_ms operator_total_ms conflict_edges k_upper k_start k_found attempts oracle_calls <<< "$operator_metrics"
        
        # 拿到 metrics（以 quantum_rpc_ms 非空为准），结束重试
        if [ -n "$quantum_rpc_ms" ]; then
            if [ $attempt -gt 1 ]; then
                print_success "第 $attempt 次尝试成功拿到 metrics"
            fi
            break
        fi

        attempt=$((attempt + 1))
    done
    
    local end_time=$(date +%s)
    local total_duration=$((end_time - start_time))
    
    if [ "$completed" = false ]; then
        print_warning "处理超时，查看 Operator 日志："
        tail -n 30 "$OPERATOR_LOG"
        # 最后再尝试抓一次 metrics（可能在超时边界写入）
        local timeout_metrics
        timeout_metrics=$(extract_operator_metrics "$OPERATOR_LOG" $((op_log_start_line + 1)) "$batch_name")
        IFS=$'\t' read -r quantum_rpc_ms extract_conflicts_ms annotate_apply_ms clear_gates_ms operator_total_ms conflict_edges k_upper k_start k_found attempts oracle_calls <<< "$timeout_metrics"
        ensure_report_trailing_newline
        echo "$(date '+%Y-%m-%d %H:%M:%S'),$test_name,$batch_name,$pod_count,0,$k_greedy,$k_dsatur,$quantum_rpc_ms,$extract_conflicts_ms,$annotate_apply_ms,$clear_gates_ms,$operator_total_ms,$conflict_edges,$k_upper,$k_start,$k_found,$attempts,$oracle_calls,$total_duration,TIMEOUT,FAILED" >> "$REPORT_FILE"
        return 1
    fi
    
    # 显示结果
    echo ""
    print_info "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    print_success "测试结果: $test_name"
    print_info "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    kubectl --server=http://localhost:3131 get pods -o wide
    
    echo ""
    print_info "性能统计："
    echo "  - 总耗时: ${total_duration}s"
    echo "  - Quantum RPC: ${quantum_rpc_ms:-N/A}ms"
    echo "  - 冲突提取: ${extract_conflicts_ms:-N/A}ms"
    echo "  - Annotation 写入: ${annotate_apply_ms:-N/A}ms"
    echo "  - Gate 清理: ${clear_gates_ms:-N/A}ms"
    echo "  - Operator 总阶段耗时: ${operator_total_ms:-N/A}ms"
    echo "  - 冲突边数: ${conflict_edges:-N/A}"
    echo "  - k_upper / k_start / k_found: ${k_upper:-N/A} / ${k_start:-N/A} / ${k_found:-N/A}"
    echo "  - attempts / oracle_calls: ${attempts:-N/A} / ${oracle_calls:-N/A}"
    echo "  - 使用节点数: $used_nodes"
    echo "  - Greedy(LF) 基线: $k_greedy"
    echo "  - DSATUR 基线: $k_dsatur"
    echo "  - Pod 分布: $pod_dist"
    
    # 记录到 CSV（若未调度则记为 FAILED）
    local status="SUCCESS"
    if [ -z "$pod_dist" ] || [ "$used_nodes" -eq 0 ]; then
        status="FAILED"
        print_warning "未检测到任何 Pod 被调度到节点，此轮结果将标记为 FAILED"
    fi
    ensure_report_trailing_newline
    echo "$(date '+%Y-%m-%d %H:%M:%S'),$test_name,$batch_name,$pod_count,$used_nodes,$k_greedy,$k_dsatur,$quantum_rpc_ms,$extract_conflicts_ms,$annotate_apply_ms,$clear_gates_ms,$operator_total_ms,$conflict_edges,$k_upper,$k_start,$k_found,$attempts,$oracle_calls,$total_duration,$pod_dist,$status" >> "$REPORT_FILE"
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
    tail -n +$((op_log_start_line + 1)) "$OPERATOR_LOG" | grep -aE "(Batch|量子|颜色|recommended-node|耗时|time|k_u|k_start|k_found|oracle_calls|下降|attempts_by_k|Operator\\]\\[Metrics\\])" | tail -n 40
    
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

    # 每 case 收尾做一次 Operator 存活检查；若已退出则自动重启。
    ensure_operator_alive
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

    # 先让用户（或环境变量）决定量子服务部署方式
    choose_quantum_service_mode

    # 执行各个步骤
    stop_old_containers
    start_containers
    start_operator
    run_test_menu
    
    print_success "感谢使用 Grover Quantum Scheduler 测试系统！"
}

# 运行主函数
main
