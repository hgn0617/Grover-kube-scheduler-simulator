FROM python:3.10-slim

WORKDIR /app

# 安装系统依赖
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# 安装Python依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制服务代码和Grover算法代码
COPY quantum_service.py .
COPY grover_graph_coloring.py .
COPY graph_analysis.py .

EXPOSE 8000

CMD ["uvicorn", "quantum_service:app", "--host", "0.0.0.0", "--port", "8000"]
