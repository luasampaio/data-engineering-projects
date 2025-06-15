# Projeto Lakehouse Stream & Batch Analytics

Este projeto demonstra uma arquitetura moderna de dados unificando processamento em streaming e batch, usando as melhores ferramentas open source do ecossistema Big Data.

## Componentes
- **API FastAPI**: Geração de dados fake (streaming via Kafka e batch via Parquet), controle e monitoramento.
- **Kafka**: Ingestão e distribuição de eventos em tempo real.
- **Flink**: Processamento streaming, agregação em janelas e persistência no Postgres.
- **Spark**: Processamento batch dos arquivos Parquet gerados.
- **Postgres**: Persistência dos dados processados.
- **Prometheus & Grafana**: Monitoramento e dashboards.
- **Kafka-UI**: Visualização dos tópicos e mensagens Kafka.

## Fluxo de Dados
```
API (Faker) → Kafka (streaming) → Flink (streaming) → Postgres
    │                                   ↑
    └── (batch Parquet) → Spark (batch) ┘
```

## Como rodar o projeto
1. **Suba todos os serviços:**
   ```sh
   docker-compose up -d
   ```
2. **Acesse os principais endpoints:**
   - API: http://localhost:8000
   - Grafana: http://localhost:3000 (admin/admin)
   - Prometheus: http://localhost:9090
   - Kafka-UI: http://localhost:8085
   - Flink Dashboard: http://localhost:8081
   - Spark Master UI: http://localhost:8080
   - Postgres: localhost:5432 (admin/admin)

3. **Dashboards Grafana:**
   - Dashboards prontos em `monitoring/grafana-provisioning/dashboards/`
   - Recomenda-se importar dashboards oficiais do Grafana Labs (links no README da pasta de dashboards)

## Endpoints úteis da API
- `POST /spammer/start` — Inicia geração de dados fake
- `POST /spammer/stop` — Para geração de dados
- `GET /spammer/status` — Status do gerador
- `POST /orders` — Insere ordem manualmente
- `POST /batch/run` — Dispara processamento batch Spark
- `GET /batch/status` — Status dos arquivos batch
- `GET /flink/status` — Status dos jobs Flink
- `GET /spark/status` — Status do Spark Master

## Teste ponta a ponta
1. Verifique dados chegando no Kafka (Kafka-UI)
2. Veja agregações em tempo real no Flink Dashboard
3. Rode o batch manualmente (`POST /batch/run` ou via Spark UI)
4. Consulte os resultados agregados no Postgres
5. Visualize métricas e dados nos dashboards Grafana

## Observabilidade
- Métricas expostas por todos os serviços principais
- Dashboards recomendados para cada componente

## Scripts e inicialização
- Tabelas do Postgres criadas automaticamente via `init_db.sql`
- Jobs Spark e Flink podem ser disparados manualmente ou automatizados

## Customização
- Ajuste a taxa de geração de dados via env `ORDER_RATE_PER_SEC`
- Adicione mais dashboards Grafana conforme necessidade

---


