from dotenv import load_dotenv
load_dotenv()

from prometheus_fastapi_instrumentator import Instrumentator
from fastapi import FastAPI, BackgroundTasks, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from faker import Faker
from kafka import KafkaProducer
import json, os, time, threading
import pandas as pd
from datetime import datetime
from prometheus_client import Counter
import subprocess
import requests

fake = Faker()
app = FastAPI(title="Orders Generator")
Instrumentator().instrument(app).expose(app, endpoint="/metrics")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
BATCH_DIR = os.getenv("BATCH_DIR", "/data/batch")
RATE_PER_SEC = int(os.getenv("ORDER_RATE_PER_SEC", "100"))

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# Prometheus counter
orders_streamed = Counter("orders_streamed_total", "Total de ordens enviadas para o Kafka")
orders_batched = Counter("orders_batched_total", "Total de ordens salvas em batch (Parquet)")

class Order(BaseModel):
    user_id: str
    amount: float
    ts: datetime

# --- REST endpoint (ad‑hoc insert) ------------------------------
@app.post("/orders")
def new_order(order: Order):
    producer.send("orders", order.dict())
    orders_streamed.inc()
    return {"status": "queued"}

# --- Controle do spammer ----------------------------------------
spammer_thread = None
spammer_running = threading.Event()

def spam_orders(rate_per_sec: int = RATE_PER_SEC):
    while spammer_running.is_set():
        batch = []
        for _ in range(rate_per_sec):
            order = {
                "user_id": fake.uuid4()[:8],
                "amount": round(fake.pyfloat(min_value=5, max_value=500), 2),
                "ts": datetime.utcnow().isoformat()
            }
            producer.send("orders", order)
            orders_streamed.inc()
            batch.append(order)
        # grava lote Parquet 1x por minuto
        if datetime.utcnow().second == 0 and batch:
            ts = datetime.utcnow().strftime("%Y%m%d%H%M")
            path = f"{BATCH_DIR}/{ts}.parquet"
            pd.DataFrame(batch).to_parquet(path, index=False)
            orders_batched.inc(len(batch))
        time.sleep(1)

@app.post("/spammer/start")
def start_spammer(rate_per_sec: int = Query(RATE_PER_SEC, description="Ordens por segundo")):
    global spammer_thread
    if spammer_running.is_set():
        return {"status": "already running"}
    spammer_running.set()
    spammer_thread = threading.Thread(target=spam_orders, args=(rate_per_sec,), daemon=True)
    spammer_thread.start()
    return {"status": "started", "rate_per_sec": rate_per_sec}

@app.post("/spammer/stop")
def stop_spammer():
    if not spammer_running.is_set():
        return {"status": "not running"}
    spammer_running.clear()
    return {"status": "stopped"}

@app.get("/spammer/status")
def spammer_status():
    return {"running": spammer_running.is_set()}

# --- Endpoint para disparar batch manual e checar status dos jobs ---
@app.post("/batch/run")
def run_spark_batch():
    try:
        result = subprocess.run([
            "spark-submit", "/app/spark_jobs/daily_etl.py"
        ], capture_output=True, text=True, timeout=300)
        return JSONResponse({
            "status": "completed" if result.returncode == 0 else "failed",
            "stdout": result.stdout[-500:],
            "stderr": result.stderr[-500:]
        })
    except Exception as e:
        return JSONResponse({"status": "error", "error": str(e)})

@app.get("/batch/status")
def batch_status():
    # Simples: verifica se último arquivo Parquet foi processado recentemente
    import glob, os
    files = glob.glob(f"{BATCH_DIR}/*.parquet")
    if not files:
        return {"status": "no batch files found"}
    last_file = max(files, key=os.path.getctime)
    return {"last_batch_file": last_file, "ctime": os.path.getctime(last_file)}

@app.get("/flink/status")
def flink_status():
    # Checa se job Flink está rodando via REST API padrão (porta 8081)
    try:
        resp = requests.get("http://jobmanager:8081/jobs")
        if resp.status_code == 200:
            jobs = resp.json().get("jobs", [])
            return {"jobs": jobs}
        else:
            return {"status": "unreachable", "http_code": resp.status_code}
    except Exception as e:
        return {"status": "error", "error": str(e)}

@app.get("/spark/status")
def spark_status():
    # Consulta o Spark Master REST API (porta 8080)
    try:
        resp = requests.get("http://spark-master:8080/json")
        if resp.status_code == 200:
            return resp.json()
        else:
            return {"status": "unreachable", "http_code": resp.status_code}
    except Exception as e:
        return {"status": "error", "error": str(e)}

# --- health ------------------------------------------------------
@app.get("/")
def ping():
    return {"msg": "running"}

@app.on_event("startup")
def _init_metrics():
    # Inicia o spammer automaticamente se desejado
    if os.getenv("SPAMMER_AUTO_START", "1") == "1":
        spammer_running.set()
        global spammer_thread
        spammer_thread = threading.Thread(target=spam_orders, args=(RATE_PER_SEC,), daemon=True)
        spammer_thread.start()