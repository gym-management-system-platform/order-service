package com.order_service.enums;


public enum OutboxEventType {
    OrderCreatedEvent("order-created"),
    OrderCompensatedEvent("order-compensated"),
    OrderProcessingPaymentEvent("order-processing-payment");

    private final String topicConfigKey;

    OutboxEventType(String topicConfigKey) {
        this.topicConfigKey = topicConfigKey;
    }

    public String topicConfigKey() {
        return topicConfigKey;
    }
}
