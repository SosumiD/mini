# ER-диаграмма (3NF; Mermaid)

```mermaid
erDiagram
    FLIGHT ||--o{ SEAT_RESERVATION : "holds seats (FK)"
    FLIGHT ||..o{ BOOKING : "logical flight_id, no cross-DB FK"
    BOOKING ||..|| SEAT_RESERVATION : "logical booking_id, no cross-DB FK"
    BOOKING_OPERATION ||..o| BOOKING : "logical ID"

    FLIGHT {
        UUID id PK
        VARCHAR airline
        VARCHAR flight_number "number + departure UTC day UNIQUE"
        VARCHAR origin "IATA 3"
        VARCHAR destination "IATA 3"
        TIMESTAMPTZ departure_at
        TIMESTAMPTZ arrival_at
        INTEGER total_seats "positive"
        INTEGER available_seats "0..total_seats"
        BIGINT price_cents "positive"
        VARCHAR status "SCHEDULED DEPARTED CANCELLED COMPLETED"
    }
    SEAT_RESERVATION {
        UUID id PK
        UUID booking_id UK "logical reference to Booking"
        UUID flight_id FK
        INTEGER seat_count "positive"
        VARCHAR status "ACTIVE RELEASED EXPIRED"
        TIMESTAMPTZ created_at
        TIMESTAMPTZ updated_at
    }
    BOOKING {
        UUID id PK
        VARCHAR user_id
        UUID flight_id "logical ref, no FK"
        VARCHAR passenger_name
        VARCHAR passenger_email
        INTEGER seat_count "positive"
        BIGINT unit_price_cents
        BIGINT total_price_cents "snapshot"
        VARCHAR status "CONFIRMED CANCELLED"
        BOOLEAN cancellation_pending "internal saga"
        TIMESTAMPTZ created_at
        TIMESTAMPTZ updated_at
    }
    BOOKING_OPERATION {
        UUID id PK "same as Booking.id if confirmed"
        UUID flight_id
        VARCHAR state "internal saga journal"
        TIMESTAMPTZ created_at
        TIMESTAMPTZ updated_at
    }
```

**Физическое размещение:** FLIGHT и SEAT_RESERVATION принадлежат только БД `flights`; BOOKING и BOOKING_OPERATION — только БД `bookings`. Стрелки с пунктиром обозначают логические связи без межбазовых внешних ключей. Связь SeatReservation.booking_id уникальна: для одного бронирования максимум одна запись резерва.

**3NF:** обычные атрибуты непосредственно зависят от первичных ключей, нет справочных групп или повторяющихся колонок. Календарная дата рейса не дублируется: уникальный функциональный индекс на `flight_number` и UTC-дату, извлечённую из `departure_at`. `available_seats` и `total_price_cents` намеренно материализованные значения (счётчик для атомарного бронирования и денежный snapshot); их согласованность защищена DB CHECK + транзакциями. Это контролируемая избыточность ради требований задания, а не отдельная зависимая справочная сущность.

**Защита целостности:** CHECK для цен, количества мест, маршрутов, времени прилёта, enum-статусов; внешние ключи только в пределах сервиса; уникальность `(flight_number, UTC departure date)` и `seat_reservations.booking_id`; `SELECT FOR UPDATE` для изменяющих операций. Применение ограничений: `services/*/alembic/versions/001_initial.py`.
