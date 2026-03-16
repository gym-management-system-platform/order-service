package com.order_service.enums;


public enum OutboxEventType {
    OrderCreatedEvent,
    OrderCompensatedEvent,
    OrderProcessingPaymentEvent
}
