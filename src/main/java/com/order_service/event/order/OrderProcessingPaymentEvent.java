package com.order_service.event.order;


import java.math.BigDecimal;


import com.order_service.event.payment.PaymentEvent;
import lombok.Getter;
import lombok.experimental.SuperBuilder;


@Getter
@SuperBuilder
public class OrderProcessingPaymentEvent extends PaymentEvent {

    private final BigDecimal amount;
    private final String currency;
}
