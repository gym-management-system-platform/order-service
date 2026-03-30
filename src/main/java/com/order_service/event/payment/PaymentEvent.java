package com.order_service.event.payment;


import com.order_service.event.base.BaseEvent;
import lombok.Getter;
import lombok.experimental.SuperBuilder;


@Getter
@SuperBuilder
public abstract class PaymentEvent extends BaseEvent {
}
