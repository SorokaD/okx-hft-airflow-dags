# OKX HFT Airflow DAG Conventions (Standard)

Единый стандарт для всех DAG-ов репозитория (raw->core, core->mart, health, retention).

---

## 1) Нейминг: DAG

### 1.1 Формат `dag_id`
`okx__<domain>__<entity>`

Примеры:
- `okx__raw_to_core__orderbook_snapshots`
- `okx__raw_to_core__orderbook_updates`
- `okx__core_to_mart__funding_rate_1m`
- `okx__health__data_freshness_5m`
- `okx__retention__weekly_cleanup`

`domain` — фиксированный словарь:
- `raw_to_core`
- `core_to_mart`
- `health`
- `retention`
- `ops`

### 1.2 Schedule
- raw_to_core: обычно `*/1 * * * *` (минутный) или `*/2`/`*/5` при больших объёмах.
- health: `*/5 * * * *`
- retention: `@weekly` или конкретный cron

### 1.3 Tags
Минимальный обязательный набор:
- `okx`
- категория: `etl` / `health` / `retention`
- слой: `raw-to-core` / `core-to-mart`
- хранилище: `timescaledb`

---

## 2) Нейминг: tasks

### 2.1 Единые task_id
- Основная синхронизация всегда: `sync`
- Backfill (если есть отдельным DAG): `backfill`
- Checks: `run_checks`
- Retention: `run_retention`

Важно: task_id должен быть коротким и одинаковым во всех DAG — это упрощает мониторинг.

---

## 3) Коннекты и самопроверка

### 3.1 Airflow Connection
- По умолчанию используем один conn_id:
  - `CONN_ID = "timescaledb"`

### 3.2 Проверка базы
Каждый DAG, который пишет в БД, обязан:
- `SELECT 1` ping
- `SELECT current_database()` и сравнить с `DB_NAME_EXPECTED = "okx_hft"`

Если имя не совпало — DAG должен падать сразу.

---

## 4) Стандарт кода DAG

### 4.1 Обязательные секции (в файле и в таком порядке)
1) Imports
2) Project-wide constants (CONN_ID, DB_NAME_EXPECTED, DAG_ID, SCHEDULE, TAGS)
3) Config dataclass (`EtlConfig`)
4) Helpers (now_utc, db_sanity_checks, watermark, sql builder, logging)
5) Main callable `run_sync()`
6) DAG definition

### 4.2 PythonOperator vs PostgresOperator
Стандарт: PythonOperator + PostgresHook.
Причины:
- единая проверка подключения
- единый лог-формат (batch/dedup/inserted)
- легче расширять проверками/метриками

---

## 5) Watermark и инкремент

### 5.1 Watermark в core
Стандарт: watermark берём из core:
- `MAX(ts_ingest)` (timestamptz) -> в ms

Если core пустой => watermark = 0.

### 5.2 Инкремент в raw
Стандарт: в raw обязана быть колонка:
- `ts_ingest_ms bigint`

Фильтр:
- `WHERE ts_ingest_ms > last_ms`
- сортировка:
- `ORDER BY ts_ingest_ms`
- `LIMIT batch_size`

---

## 6) Dedup стратегия (стандарт)

### 6.1 Почему дедуп внутри батча
В raw могут быть повторы (повторная публикация снапшота/сетевых событий).
Timescale часто не позволяет удобный UNIQUE, а неправильный UNIQUE режет данные.
Поэтому стандарт по умолчанию:

- Берём `batch`
- Делаем `dedup`:
  - `row_number() over (partition by <semantic_key> order by ts_ingest_ms desc) = 1`
- Вставляем только `dedup`

### 6.2 Semantic key
Определяется на уровне сущности, но правило такое:
- ключ должен описывать "одну логическую запись", не "снимок целиком".

Пример snapshots:
- `(instid, ts_event_ms, side, level)`

Пример updates:
- зависит от формата updates; как минимум `(instid, ts_event_ms, side, price)` или иной подтверждённый ключ.

---

## 7) Логирование (обязательное)

Каждый run должен печатать одну строку summary:
- `loops`
- `batch`
- `dedup`
- `inserted`
- `last_ms`
- `db`
- `now_utc`

Формат (пример):
`[okx__raw_to_core__orderbook_snapshots] now_utc=... db=... loops=.. batch=.. dedup=.. inserted=.. last_ms=..`

---

## 8) Параметры устойчивости

Обязательные настройки DAG:
- `catchup=False`
- `max_active_runs=1`

Обязательные default_args:
- `execution_timeout` (обычно 120–300 секунд)
- `retries` (обычно 2)
- `retry_delay` (обычно 60 секунд)

---

## 9) Backfill

Стандарт:
- backfill выполняется отдельно (ручной запуск), либо тем же DAG при `max_loops` увеличенном.
- при backfill всегда логируем прогресс (inserted).
- если нужно "чисто": TRUNCATE core перед первым backfill (ручной операцией).

---

## 10) Индексы (минимальный стандарт)

RAW:
- индекс на `ts_ingest_ms`
- (опционально) индекс под аналитические запросы `(instid, ts_event_ms desc)`

CORE:
- индекс под основные чтения `(inst_id, ts_event desc)`
- индекс `ts_ingest` для watermark/диагностики

Дубликатные индексы запрещены. Перед добавлением индекса — проверить `pg_indexes`.

---

## 11) Принципы изменения схем

- Не вводить UNIQUE/PK на hypertable без проверки реальных дублей.
- Если данные "не идеально чистые" — дедуп делаем на входе в core.
- Схема core должна быть удобна для чтения/аналитики, а не только для “красивых constraints”.

---

## 12) Definition of Done для нового ETL DAG

Перед тем как считать DAG “готовым”:
1) Есть самопроверка DB.
2) В логах видно batch/dedup/inserted.
3) Core rows растут, lag (raw_max - core_max) разумный.
4) Нет необъяснимой потери объёма (например, core в 100 раз меньше raw).
5) Индексы без дублей.

