package com.order_service.event.order;


import lombok.Getter;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

import java.math.BigDecimal;
import java.util.UUID;


@Jacksonized
@Getter
@SuperBuilder
public class OrderCreatedEvent extends OrderEvent {
    private final BigDecimal amount;
    private final UUID productId;
    private final Integer quantity;
}
