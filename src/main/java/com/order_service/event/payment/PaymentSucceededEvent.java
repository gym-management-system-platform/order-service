package com.order_service.event.payment;


import lombok.Getter;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

import java.util.UUID;

@Jacksonized
@Getter
@SuperBuilder
public class PaymentSucceededEvent extends PaymentEvent {

    private final UUID paymentId;
}

