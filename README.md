# OKX HFT Airflow DAGs

Production Airflow DAGs для **Tumar.tech OKX HFT Platform**.  
Деплоятся через **git-sync** в Docker Airflow (Airflow 2.10+).

## Назначение

Этот репозиторий содержит DAG-и для оркестрации data pipelines OKX HFT платформы:

| Pipeline | Описание | Schedule |
|----------|----------|----------|
| `okx_health_checks` | Проверка работоспособности системы | `*/5 * * * *` |
| `okx_raw_to_core` | Репликация raw -> core таблиц (trades, orderbook, funding) | `*/1 * * * *` |
| `okx_feature_engineering` | Расчёт фич для ML моделей | `@hourly` |
| `okx_data_quality` | Проверки качества данных (gaps, duplicates) | `@daily` |
| `okx_retention` | Очистка старых данных и агрегация | `@weekly` |

## Архитектура платформы

```
OPS Node (tumar.tech)
  - Airflow :8080
  - Grafana :3000  
  - MLflow :5000
  - Superset :8088
  - Traefik :443
  |
  | git-sync (этот репозиторий)
  v
TimescaleDB Node (167.86.110.201)
  - okx_raw (hypertables)
  - okx_core (hypertables)
  - okx_feat (hypertables)
  ^
  | INSERT via asyncpg
  |
Collector Node (217.216.73.20)
  - okx-hft-collector
  - WebSocket -> OKX API -> okx_raw.* tables
```

## Структура репозитория

```
okx-hft-airflow-dags/
  dags/
    okx/
      common/           # Общий код
        settings.py     # DEFAULT_ARGS, OWNER, TZ
        alerting.py     # Telegram/Slack уведомления
        utils.py        # Хелперы (date ranges, SQL templates)
      pipelines/        # DAG-файлы
        okx_health_checks.py
        okx_raw_to_core.py
  tests/
    test_import_dags.py # Проверка импорта всех DAG-ов
  scripts/
    lint.sh             # ruff + black
    test.sh             # pytest
  .github/workflows/
    ci.yml              # GitHub Actions
  pyproject.toml        # Python config
  requirements-dev.txt  # Dev dependencies
  README.md
```

## Конфигурация

### Airflow Connections (настраиваются в UI)

| Connection ID | Тип | Описание |
|---------------|-----|----------|
| `timescaledb` | Postgres | TimescaleDB (167.86.110.201:5432) |
| `telegram_bot` | HTTP | Telegram Bot API для алертов |
| `minio_s3` | S3 | MinIO для артефактов (s3.tumar.tech) |

### Airflow Variables

| Variable | Описание | Пример |
|----------|----------|--------|
| `OKX_INSTRUMENTS` | Торгуемые инструменты | `["BTC-USDT-SWAP"]` |
| `DQ_ALERT_THRESHOLD` | Порог для DQ алертов | `0.01` |

## Правила разработки

### 1. Никаких секретов в коде

```python
# ПЛОХО
conn_string = "postgresql://user:password@host:5432/db"

# ХОРОШО
from airflow.hooks.base import BaseHook
conn = BaseHook.get_connection("timescaledb")
```

### 2. DAG-и должны быть тонкими

```python
# ПЛОХО - бизнес-логика в DAG
@task
def process_data():
    df = pd.read_sql("SELECT * FROM raw_table", conn)
    df["feature"] = df["price"].rolling(100).mean()
    df.to_sql("features", conn)

# ХОРОШО - вызов SQL через Hook
@task
def replicate_raw_to_core():
    hook = PostgresHook(postgres_conn_id="timescaledb")
    hook.run("""
        INSERT INTO okx_core.trades
        SELECT * FROM okx_raw.trades
        WHERE ts_ingest_ms > {{ params.last_ts }}
    """)
```

### 3. Используй TaskFlow API

```python
from airflow.decorators import dag, task

@dag(...)
def my_pipeline():
    @task
    def extract(): ...
    
    @task
    def transform(data): ...
    
    @task
    def load(data): ...
    
    load(transform(extract()))
```

### 4. Добавляй tags для фильтрации

```python
@dag(
    dag_id="okx_raw_to_core",
    tags=["okx", "etl", "core"],
    ...
)
```

## Как добавить новый DAG

1. **Создай файл** в `dags/okx/pipelines/my_pipeline.py`

2. **Используй шаблон:**

```python
from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from okx.common.settings import DEFAULT_ARGS

@dag(
    dag_id="okx_my_pipeline",
    default_args=DEFAULT_ARGS,
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["okx", "my-feature"],
    doc_md="""
    ### My Pipeline
    Описание что делает DAG.
    """,
)
def my_pipeline():
    @task
    def my_task():
        hook = PostgresHook(postgres_conn_id="timescaledb")
        hook.run("SELECT 1")
        print("Task completed")
    
    my_task()

my_pipeline()
```

3. **Проверь импорт:**

```bash
pytest tests/test_import_dags.py -v
```

4. **Запусти линтер:**

```bash
bash scripts/lint.sh
```

5. **Создай PR** -> CI проверит -> merge в main -> git-sync подхватит

## Локальная разработка

```bash
# Клонировать репозиторий
git clone git@github.com:tumar-tech/okx-hft-airflow-dags.git
cd okx-hft-airflow-dags

# Создать виртуальное окружение
python3.11 -m venv venv
source venv/bin/activate

# Установить зависимости
pip install -r requirements-dev.txt

# Линтинг
bash scripts/lint.sh

# Тесты
bash scripts/test.sh
```

## CI/CD

### GitHub Actions

На каждый push и pull_request:
1. Checkout кода
2. Setup Python 3.11
3. Install dependencies
4. Lint (ruff, black)
5. Test (pytest import check)

### Git-sync

Airflow Worker настроен на синхронизацию:

```yaml
# docker-compose.yaml (в okx-hft-ops)
git-sync:
  image: registry.k8s.io/git-sync/git-sync:v4.2.1
  environment:
    GITSYNC_REPO: https://github.com/tumar-tech/okx-hft-airflow-dags.git
    GITSYNC_BRANCH: main
    GITSYNC_ROOT: /dags
    GITSYNC_PERIOD: 60s
```

После push в `main` - DAG-и появятся в Airflow UI через 1 минуту.

## Мониторинг

- **Airflow UI**: https://airflow.tumar.tech
- **Grafana Dashboard**: https://grafana.tumar.tech/d/airflow
- **Logs**: Loki -> Grafana (label: `container_name=~"airflow.*"`)

## Связанные репозитории

| Репозиторий | Описание |
|-------------|----------|
| okx-hft-ops | Infrastructure (Docker, Traefik, Monitoring) |
| okx-hft-collector | WebSocket collector -> TimescaleDB |
| okx-hft-timescaledb | Database schema, migrations |
| okx-hft-executor | Trade execution service |

## Контакты

- **Team**: OKX Data Team
- **Owner**: okx-data
