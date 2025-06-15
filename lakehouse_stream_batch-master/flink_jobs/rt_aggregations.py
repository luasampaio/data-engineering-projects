from pyflink.table import EnvironmentSettings, TableEnvironment
import os
from dotenv import load_dotenv
load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "demo")
POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin")

settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(settings)

# Source Kafka
t_env.execute_sql(f"""
CREATE TABLE orders (
  user_id STRING,
  amount DOUBLE,
  ts     TIMESTAMP_LTZ(3),
  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders',
  'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
  'format' = 'json',
  'scan.startup.mode' = 'earliest-offset'
)
""")

# Sink Postgres (JDBC upsert)
t_env.execute_sql(f"""
CREATE TABLE orders_rt (
  window_start TIMESTAMP_LTZ(3),
  window_end   TIMESTAMP_LTZ(3),
  user_id      STRING,
  gmv          DOUBLE,
  order_cnt    BIGINT,
  PRIMARY KEY (window_start, user_id) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}',
  'table-name' = 'orders_rt',
  'username' = '{POSTGRES_USER}',
  'password' = '{POSTGRES_PASSWORD}'
)
""")

t_env.execute_sql("""
INSERT INTO orders_rt
SELECT
  window_start,
  window_end,
  user_id,
  SUM(amount) AS gmv,
  COUNT(*)    AS order_cnt
FROM TABLE(
  TUMBLE(TABLE orders, DESCRIPTOR(ts), INTERVAL '5' MINUTES)
)
GROUP BY window_start, window_end, user_id
""")
