package com.order_service.event.order;


import com.order_service.enums.Currency;
import lombok.Getter;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

import java.math.BigDecimal;


@Jacksonized
@Getter
@SuperBuilder
public class OrderProcessingPaymentEvent extends OrderEvent {

    private final BigDecimal amount;
    private final Currency currency;
}
