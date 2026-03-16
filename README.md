# orderEntity-service

# 1. Цель проекта

Создать сервис для управления жизненным циклом заказов на покупку тренировочных пакетов.
Сервис является инициатором (Orchestrator) распределенной транзакции (Saga) и координирует взаимодействие других
сервисов.
---

# 2. Функциональные требования

## 2.1. Сущности системы

### Заказ (Order)

- **id** (обязательно) — уникальный идентификатор заказа.
- **userId** (обязательно) — идентификатор клиента (может приходить из запроса или из токена).
- **packageId** (обязательно) — идентификатор приобретаемого пакета (например, "PRO_PACK_10").
- **packageName** (обязательно) — человекочитаемое название пакета.
- **amount** (обязательно) — стоимость заказа.
- **status** (обязательно) — статус заказа: PENDING, COMPLETED, CANCELLED, FAILED.
- **sagaId** (опционально) — идентификатор саги для отслеживания.
- **createdAt** (обязательно) — дата создания.
- **updatedAt** (обязательно) — дата последнего обновления статуса.

---

## 2.2. Операции API

### Для упражнений (Exercises)

- inventory-service: `POST /api/v1/reservations` — для резервирования ресурсов.
- payment-service: `POST /api/v1/charges` — для списания средств.
- schedule-service: `POST /api/v1/bookings` — для бронирования стартовой сессии.
- notification-service: `POST /api/v1/notifications` — для отправки уведомления.

---

## 2.3. Создание упражнения

`POST /api/v1/orders` — Создание нового заказа и запуск Saga.

**Тело запроса:**

```json
{
  "userId": "user-123",
  "packageId": "GOLD_PACKAGE",
  "packageName": "Золотой пакет (10 тренировок + инвентарь)",
  "amount": 299.99
}
```

**Ответ:**

```json
{
  "orderId": 123,
  "status": "PENDING",
  "message": "Order processing started"
}
```

## 3. Бизнес-логика (Saga Orchestration)

1. Создание заказа со статусом PENDING.
2. Вызов inventory-service для резервирования. При ошибке → статус CANCELLED.
3. Вызов payment-service для оплаты. При ошибке → компенсация: вызов inventory-service для отмены резерва → статус
   CANCELLED.
4. Вызов schedule-service для бронирования. При ошибке → компенсация: вызов payment-service для возврата и
   inventory-service для отмены → статус CANCELLED.
5. Вызов notification-service для уведомления.
6. Обновление статуса заказа на COMPLETED.

## 4. Технологический стек

- Java 17+
- Spring Boot 3.x
- Spring Data JPA
- PostgreSQL
- Lombok
- MapStruct
- Swagger / OpenAPI 3
- Jakarta Validation
- SLF4j