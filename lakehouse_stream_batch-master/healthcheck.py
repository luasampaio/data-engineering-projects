import requests
import sys

def check(name, url, expect_json=True):
    try:
        resp = requests.get(url, timeout=5)
        if resp.status_code == 200:
            if expect_json:
                print(f"[OK] {name}: {resp.json()}")
            else:
                print(f"[OK] {name}: {resp.text[:100]}")
            return True
        else:
            print(f"[FAIL] {name}: HTTP {resp.status_code}")
            return False
    except Exception as e:
        print(f"[FAIL] {name}: {e}")
        return False

def main():
    checks = [
        ("API", "http://localhost:8000/"),
        ("Prometheus", "http://localhost:9090/-/healthy", False),
        ("Grafana", "http://localhost:3000/api/health"),
        ("Kafka-UI", "http://localhost:8085"),
        ("Flink Dashboard", "http://localhost:8081"),
        ("Spark Master UI", "http://localhost:8080"),
        ("API/Spammer Status", "http://localhost:8000/spammer/status"),
        ("API/Batch Status", "http://localhost:8000/batch/status"),
        ("API/Flink Status", "http://localhost:8000/flink/status"),
        ("API/Spark Status", "http://localhost:8000/spark/status"),
    ]
    failed = False
    for name, url, *rest in checks:
        expect_json = rest[0] if rest else True
        if not check(name, url, expect_json):
            failed = True
    if failed:
        print("\nAlgum serviço está offline ou com problemas!")
        sys.exit(1)
    else:
        print("\nTodos os serviços principais estão online!")

if __name__ == "__main__":
    main()
