# Online Cinema — Kafka → ClickHouse → PostgreSQL → Grafana / S3

Готовый учебный проект на Python 3.12 / FastAPI, выполняющий пункты 1–8 задания (до 10 баллов по рубрике). **Docker Compose нужен локально**. Не содержит заглушек и не требует ручного SQL или регистрации схем.

## Быстрый запуск (Windows PowerShell, Git Bash, Linux)

1. Откройте Docker Desktop и убедитесь, что Docker Engine работает. Рекомендуется **8–12 ГБ доступной RAM**, так как здесь 3 брокера Kafka, ClickHouse, PostgreSQL и Grafana.
2. Откройте терминал в папке проекта и выполните одну команду:

```bash
docker compose up --build
```

На первом запуске Docker скачает образы и соберёт Python-контейнеры. `init` самостоятельно создаст Kafka topic, зарегистрирует Avro-схему **v1**, выполнит миграции двух БД. Producer начнёт генерировать историю просмотров за 12 дней и затем поток новых событий. Aggregator автоматически пересчитает дни истории и экспортирует агрегаты за вчера. Начальное наполнение и агрегация происходят после старта сервисов, поэтому графики появляются не мгновенно.

Для запуска без лишнего вывода: `docker compose up --build -d`.

## Куда заходить

| Сервис | Адрес | Для чего |
|---|---|---|
| Producer | http://localhost:8000/docs | HTTP API `POST /events` |
| Aggregator | http://localhost:8001/docs | `POST /aggregate/{date}`, `POST /export/{date}` |
| Grafana | http://localhost:3000/d/cinema-analytics | Дашборд (admin / admin) |
| MinIO Console | http://localhost:9001 | Файлы S3 (cinema / cinema_password) |
| Schema Registry | http://localhost:8081/subjects/movie-events-value/versions/latest | Avro-схема и версия |
| ClickHouse | http://localhost:8123 | SQL-интерфейс |

Порты: Kafka host `19092`; PostgreSQL host `5433` (cinema / cinema_password / movie_analytics); ClickHouse native host `9009` (default / cinema_pass).

## Проверка одной командой

Можно запустить тесты после запуска сервисов либо позволить Compose поднять необходимые зависимости автоматически:

```bash
docker compose --profile test run --rm tests
```

В тесте: проверяются Schema Registry и Kafka replication, создаётся уникальная последовательность событий через HTTP, ожидание в ClickHouse, корректность полей, ручная агрегация, повторный UPSERT без дублей, экспорт и чтение результата из S3, проверка ошибок валидации. Данные теста изолируются случайными UUID — один и тот же тест запускается повторно.

## Отправить своё событие

**Git Bash или Linux:**

```bash
curl -X POST http://localhost:8000/events \
  -H "Content-Type: application/json" \
  -d '{"user_id":"alice","movie_id":"movie-1","event_type":"VIEW_STARTED","timestamp":"2026-11-25T12:00:00Z","device_type":"MOBILE","session_id":"s1","progress_seconds":0}'
```

`event_id` можно не передавать — сервер создаст UUID. Для проверки в ClickHouse:

```bash
docker compose exec clickhouse clickhouse-client --user default --password cinema_pass --query "SELECT count() FROM cinema.raw_events"
```

Пересчитать сегодняшнюю дату (подставьте текущую дату UTC):

```bash
curl -X POST http://localhost:8001/aggregate/2026-11-25
```

Экспортировать её:

```bash
curl -X POST http://localhost:8001/export/2026-11-25
```

MinIO: `movie-analytics/daily/YYYY-MM-DD/aggregates.csv`. Повторный экспорт заменяет существующий объект по тому же ключу.

## Архитектура

```text
Producer (FastAPI + генератор) -- Avro --> Kafka (3 брокера, topic movie-events)
                                            |
                                            v
                      ClickHouse Kafka Engine + Materialized View
                                            |
                                            v
                      ClickHouse raw_events (ReplacingMergeTree)
                                            |
                                            v
                      Aggregation Service (расписание / HTTP)
                         |               |                |
                         v               v                v
                  CH daily_metrics  CH cohort_retention  PostgreSQL aggregates
                         |               |                |
                         +------ Grafana -+                v
                                                     MinIO (CSV/S3)
```

Ключ Kafka — `user_id`: события одного пользователя попадают в одну партицию, что сохраняет их порядок в пределах одной партиции. **Три партиции, replication.factor=3, min.insync.replicas=2, acks=all**. Три KRaft-брокера позволяют пережить отказ одного брокера при достаточном ISR. Kafka KRaft в совмещённом режиме broker/controller выбран **только для учебного стенда**, для продакшена роли следует разделять.

Schema Registry хранит Avro v1 в `schemas/movie_event.avsc`. Producer сериализует события в формате **Confluent Avro** (wire format со schema ID), а ClickHouse читает их через `AvroConfluent`. При следующей версии добавляйте совместимые поля с default-значениями, проверяя правила совместимости в Schema Registry.

ClickHouse сырьё `raw_events` партиционировано по месяцам, отсортировано `(event_date, user_id, event_id)`. `ReplacingMergeTree` с `FINAL` в запросах агрегации уменьшает последствия повторной обработки Kafka (at-least-once); материализованные `daily_metrics` и `cohort_retention` обновляются через версии (`ReplacingMergeTree(version)`).

## Формулы метрик

- **DAU:** `uniqExact(user_id)` среди всех событий за день.
- **Среднее время просмотра:** `avgIf(progress_seconds, VIEW_FINISHED)`.
- **Топ фильмов:** `countIf(VIEW_STARTED)` по каждому фильму; топ 10 выбирается на дашборде.
- **Конверсия:** количество `VIEW_FINISHED` / количество `VIEW_STARTED` × 100%. Это **отношение событий**, а не уникальных пользователей; в учебных данных один старт относится к одной сессии.
- **Retention D1, D7:** когорта — дата первого `VIEW_STARTED` пользователя. Возвращение — ещё один `VIEW_STARTED` в соответствующий календарный день. `% = вернувшиеся / размер когорты × 100`. Будущие дни в heatmap не показаны, а не считаются нулём.

Сервис пересчитывает метрики по расписанию (`AGG_INTERVAL_SECONDS`), а `POST /aggregate/YYYY-MM-DD` запускает пересчёт вручную. Лог: `cycle started`, `cycle finished`, количество событий и длительность. При записи в PostgreSQL используется `INSERT ... ON CONFLICT DO UPDATE` и retry с backoff. В PostgreSQL содержатся готовые метрики с PK `(day,metric,dimension)`.

**Grafana** подключена к ClickHouse автоматически и использует **только агрегированные таблицы**, не `raw_events`. Дашборд: цветовая таблица Retention D0–D7 (когортная heatmap), DAU, конверсия, топ фильмов и среднее время просмотра.

**Экспорт S3** встроен в Aggregation Service (без ещё одного сервиса ради одной задачи): идёт из PostgreSQL, формируется CSV с датой и временем расчёта, объект перезаписывается. Ночной экспорт настраивается тем же циклом; если S3/PG недоступны, ошибка логируется и следующий цикл повторит попытку. Ручной endpoint позволяет экспортировать любую дату с готовыми метриками.

## Проверить отказоустойчивость Kafka

После запуска данных можно проверить параметры и выключить один из трёх брокеров:

```bash
docker compose exec kafka1 kafka-topics --bootstrap-server kafka1:9092 --describe --topic movie-events
docker compose stop kafka3
```

Оставшиеся два брокера сохраняют кворум контроллеров и ISR≥2 (если оба были синхронны). Проверяйте HTTP-публикацию и появление событий в ClickHouse. Затем восстановите брокер: `docker compose start kafka3`.

## Состав файлов

```text
app/shared.py          схема входного события и ClickHouse-клиент
app/init.py            автосоздание Kafka topic, Schema Registry v1 и SQL-миграции
app/producer.py        HTTP API + Kafka producer + генератор событий
app/aggregator.py      метрики, cohort retention, PostgreSQL UPSERT, S3 export
schemas/movie_event.avsc
sql/clickhouse.sql     Kafka Engine, MergeTree, materialized view и метрики
sql/postgres.sql       таблица агрегатов
Grafana/...            provisioning datasource и готовый dashboard
 tests/test_pipeline.py интеграционные тесты
```

---

Остановить без удаления данных: `docker compose down`. Удалить и начать с нуля: `docker compose down -v`.
