# Проектное решение и компромиссы

## Взаимодействие

```mermaid
sequenceDiagram
    participant C as REST client
    participant B as Booking Service
    participant DB as Booking PostgreSQL
    participant F as Flight gRPC
    participant FD as Flight PostgreSQL
    participant R as Redis Sentinel
    C->>B: POST /bookings
    B->>F: GetFlight(id) + API key
    F->>R: GET flight:id
    alt miss
        F->>FD: SELECT flight
        F->>R: SETEX flight:id
    end
    F-->>B: Flight + price_cents
    B->>DB: INSERT booking_operations(RESERVING)
    B->>F: ReserveSeats(flight_id, count, booking_id)
    F->>FD: BEGIN; SELECT flight FOR UPDATE
    F->>FD: UPDATE available + INSERT reservation; COMMIT
    F->>R: DELETE flight:id & search keys
    F-->>B: ACTIVE reservation
    B->>DB: BEGIN; INSERT booking + CONFIRMED operation; COMMIT
    B-->>C: 201 confirmed + snapshot price
```

## Обработка сбоев

- В **Flight Service** уменьшение/возврат счётчика и запись/обновление резервации коммитятся в одной локальной транзакции. Дубликат `booking_id` имеет уникальный индекс. Повтор ACTIVE с совпадающими параметрами ничего не изменяет; повтор RELEASED ничего не добавляет.
- В **Booking Service** запись `Booking` создаётся только после успешной резервации. До RPC сохраняется отдельная внутренняя запись `BookingOperation`. В случае ошибки `ReserveSeats` (включая таймаут, когда результат неизвестен) вызывается компенсирующий `ReleaseReservation`. Неуспешная компенсация сохраняется и повторяется reconciler.
- При ошибке коммита `Booking` результат неопределён. Reconciler спустя 45 секунд проверяет, есть ли `Booking` с таким ID: если есть, фиксирует `CONFIRMED`, если нет — освобождает места.
- Отмена сначала помечает `cancellation_pending`, затем освобождает места в Flight Service и только после этого ставит `CANCELLED`. При сбое pending сохраняется и reconciler повторяет Release (идемпотентен).
- Между **двумя независимыми PostgreSQL** нет глобальной ACID-транзакции: это **saga с eventual consistency**, а не математически атомарный коммит двух БД. При сетевом разделении возможны временные расхождения, компенсируемые после восстановления связи. Для банковского production-процесса нужны более жёсткие гарантии и продуманные сроки/идемпотентность операций.

## Защита

- `x-api-key` во всех gRPC методах, секрет хранится в переменной окружения `GRPC_API_KEY`. При отсутствии/ошибке — `UNAUTHENTICATED`.
- gRPC не опубликован на хост-портах и доступен только в Docker-сети. В учебном решении HTTP/2 без TLS внутри Docker; для производства нужны mTLS, secrets management, авторизация REST пользователей, rate limiting, audit logs и мониторинг.

## Redis и отказоустойчивость

- Три Sentinel (`quorum=2`), primary и replica. Клиент `redis.sentinel.Sentinel.master_for` узнаёт текущего primary и переподключается после failover.
- Cache-Aside: `GetFlight` ключ `flight:{id}`, поиск `search:{origin}:{destination}:{date|*}`; TTL по умолчанию 360 секунд. `HIT`, `MISS`, `INVALIDATE` пишутся в лог.
- После **каждой успешной мутации** сбрасываются ключ детали, поиска с датой и поиска без даты.
- При недоступности Redis чтение выполняется из PostgreSQL. Это повышает доступность, но при разделении сети или гонках конкурентных чтений и инвалидирования возможны **кратковременные устаревшие записи** на время TTL. Сильная согласованность потребовала бы версионированных cache keys/строгой последовательности публикации изменений.
- Sentinel управляет доступностью **кеша**, а не PostgreSQL. БД в учебном compose не настроены с HA.

## Retry и circuit breaker

- Не более **трёх попыток**; интервалы между ними **100, 200 мс** (потенциальная третья задержка 400 мс применялась бы перед четвёртой попыткой, что запрещено правилом 3 попыток).
- Повтор только `UNAVAILABLE` / `DEADLINE_EXCEEDED`; ошибки бизнес-валидации не повторяются.
- Circuit Breaker вынесен в gRPC `UnaryUnaryClientInterceptor`; состояния `CLOSED` → `OPEN` → `HALF_OPEN`; один пробный вызов; параметризация через env. В режиме `OPEN` отказ без сетевого вызова и REST `503`.
