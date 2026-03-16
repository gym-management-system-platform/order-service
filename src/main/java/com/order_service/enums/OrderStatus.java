package com.order_service.enums;


public enum OrderStatus {
    PENDING,
    RESERVING_INVENTORY,
    PROCESSING_PAYMENT,
    SCHEDULING,
    COMPLETED,
    CANCELLED,
    FAILED
}
