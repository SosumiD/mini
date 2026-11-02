# Flight Booking — gRPC + PostgreSQL + Redis Sentinel

Учебный проект по заданию **«Flight Booking: gRPC + Redis»**, реализующий пункты 1–10 (см. статус тестирования ниже). Язык: Python 3.12; FastAPI, gRPC, SQLAlchemy 2, Alembic, PostgreSQL 16, Redis 7.4.

## Архитектура

```text
                     HTTP REST :8000
   Client ────────────▶ Booking Service
                         │    │ PostgreSQL "bookings"
                         │    └─ Booking + BookingOperation
                         │
                         │ gRPC :50051 + x-api-key
                         │ retry / circuit breaker interceptor
                         ▼
                      Flight Service
                         │        │
            PostgreSQL "flights"      Redis Sentinel (3 nodes)
             Flight / SeatReservation  ↕
                                     Redis primary + replica
```

У каждого сервиса отдельная PostgreSQL. На хост открыт только REST 8000; gRPC и базы доступны исключительно внутри Docker. `.proto` самостоятельно разработан; Python-классы генерируются `protoc` во время сборки. Диаграмма: [docs/ER.md](docs/ER.md); транзакции и компромиссы: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Запуск с нуля

Понадобятся Docker Engine/Desktop и Compose. В каталоге проекта:

```bash
cp .env.example .env
# Замените GRPC_API_KEY на собственный длинный случайный ключ.
docker compose up --build
```

После однократной настройки окружения весь стек запускается одной командой `docker compose up` (или `docker-compose up` для standalone Compose). Две миграции Alembic применяются автоматически. Создаются 4 демонстрационных рейса (2026-12-01 — 2026-12-03).

**REST и Swagger:** http://localhost:8000/docs, проверка: http://localhost:8000/health.

Команда PowerShell для создания `.env`: `Copy-Item .env.example .env`; запуск аналогичный `docker compose up --build`.

## Примеры REST API

`GET /flights?origin=SVO&destination=LED&date=2026-12-01` — поиск; `date` необязателен. `GET /flights/{id}` — детали.

```bash
curl "http://localhost:8000/flights?origin=SVO&destination=LED&date=2026-12-01"
curl http://localhost:8000/flights/11111111-1111-4111-8111-111111111111
```

Создание бронирования (цена фиксируется на момент `GetFlight`; денежные суммы в ответе — строки с двумя знаками, валюта RUB):

```bash
curl -X POST http://localhost:8000/bookings \
  -H "Content-Type: application/json" \
  -d '{"user_id":"user-1","flight_id":"11111111-1111-4111-8111-111111111111","passenger_name":"Ivan Petrov","passenger_email":"ivan@example.org","seat_count":2}'
```

В ответе поле `id` содержит UUID нового бронирования. Затем:

```bash
curl http://localhost:8000/bookings/BOOKING_UUID
curl "http://localhost:8000/bookings?user_id=user-1"
curl -X POST http://localhost:8000/bookings/BOOKING_UUID/cancel
```

При несуществующем рейсе — HTTP 404, нехватке мест — 409, недоступности Flight Service/OPEN circuit — 503. Ошибки валидации FastAPI возвращают 422; gRPC `INVALID_ARGUMENT` транслируется в 400.

## Прогоны тестов

После запуска контейнеров в отдельном терминале:

```bash
# Юнит-тесты переходов Circuit Breaker

docker compose exec booking-service python -m pytest -q tests/test_circuit_breaker.py

# Граница API, резервирование, счётчик, отмена и поиск
python scripts/smoke_test.py

# Глобальная аутентификация RPC и гонка за последнее место:
docker compose exec booking-service python scripts/grpc_checks.py
```

`grpc_checks.py` использует отдельный одно-местный рейс `SU0001`. Запускайте его на неизменённой демонстрационной БД, без других бронирований этого рейса.

Для просмотра cache HIT/MISS и переходов breaker: `docker compose logs -f flight-service booking-service`. Проверка отказа Redis: `docker compose stop redis-master` и просмотр `docker compose logs -f redis-sentinel-1 flight-service` (после выбора replica primary; во время failover запросы идут напрямую в PostgreSQL). Для проверки Circuit Breaker: остановить `flight-service`, выполнить 5+ запросов `GET /flights/{id}` и посмотреть переход в OPEN, потом через `CB_OPEN_SECONDS` проверить HALF_OPEN.

## Соответствие критериям

| Пункт | Реализация |
|---|---|
| 1. .proto | `shared/proto/flight.proto`: 5 бизнес-RPC, Timestamp, enums, коды ошибок, protoc |
| 2. 3NF | `docs/ER.md` + ограничения в Alembic; независимые схемы |
| 3. PostgreSQL/REST/gRPC | 2 PostgreSQL, 2 сервиса, автоприменение миграций |
| 4. Межсервисные вызовы | GetFlight → ReserveSeats → Booking, ReleaseReservation на отмену |
| 5. Транзакции | `SELECT FOR UPDATE`, единые транзакции в Flight Service, компенсирующая saga в Booking |
| 6. Аутентификация | API Key в metadata и server interceptor на **всех RPC** |
| 7. Redis Cache-Aside | flight/search keys, TTL 360s, invalidation, hit/miss |
| 8. Retry | 3 попытки, backoff 100/200мс, только UNAVAILABLE/DEADLINE_EXCEEDED, идемпотентный booking_id |
| 9. Redis HA | 1 primary + 1 replica + 3 Sentinel, Sentinel-aware клиент |
| 10. Circuit Breaker | client-side gRPC interceptor, 3 состояния, env конфигурация, логи и REST 503 |

## Важные оговорки

- **В данной среде авторской сборки Docker недоступен**, поэтому end-to-end выполнение контейнеров здесь **не проводилось**. Не следует принимать код как подтверждённо успешно сданную работу: перед сдачей запустите команды тестирования выше и проверьте логи.
- Базы **раздельные**, поэтому глобальной ACID-транзакции нет: используется saga c durable-журналом и периодической компенсацией. Это принципиальная особенность любой реализации без распределённого commit-протокола. Повтор `ReserveSeats` с тем же `booking_id` не резервирует повторно.
- Cache-Aside + Redis Sentinel обеспечивает отказоустойчивость кеша, но не строгую согласованность при одновременных чтениях и инвалидировании или длительном сетевом разделении.
- В учебном варианте нет внешнего OAuth для REST, mTLS между сервисами и HA для PostgreSQL. API Key передаётся в незашифрованном gRPC внутри приватной Compose-сети. За пределами учебной Docker-сети необходим TLS и политика секретов.
- Seed-данные привязаны к декабрю 2026; для будущих проверок замените даты миграции/запросов либо создайте собственные рейсы.
