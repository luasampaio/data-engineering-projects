# Dashboards Grafana Provisionados

Esta pasta contém dashboards prontos para monitoramento dos principais componentes do pipeline:

- Kafka
- Flink
- Spark
- PostgreSQL
- API (Prometheus)

Os arquivos JSON podem ser customizados e importados manualmente pelo Grafana, ou provisionados automaticamente via configuração.

Sugestão: baixe dashboards oficiais ou utilize exemplos do Grafana Labs para cada serviço.

Exemplo de dashboards recomendados:
- [Kafka Overview](https://grafana.com/grafana/dashboards/721)
- [Flink Dashboard](https://grafana.com/grafana/dashboards/12559)
- [Spark Monitoring](https://grafana.com/grafana/dashboards/11784)
- [Postgres Database](https://grafana.com/grafana/dashboards/9628)
- [Prometheus Stats](https://grafana.com/grafana/dashboards/3662)

Para provisionamento automático, adicione uma configuração como abaixo em `monitoring/grafana-provisioning/dashboards/all.yml`:

```yaml
dashboardProviders:
  - name: 'default'
    orgId: 1
    folder: ''
    type: file
    options:
      path: /etc/grafana/provisioning/dashboards
```

E certifique-se de mapear o volume no docker-compose:
```yml
- ./monitoring/grafana-provisioning/dashboards:/etc/grafana/provisioning/dashboards
```
