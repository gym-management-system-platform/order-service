package com.order_service.event.payment;


import lombok.Getter;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

@Jacksonized
@Getter
@SuperBuilder
public class PaymentFailedEvent extends PaymentEvent {

    private final String reason;
}
