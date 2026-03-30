package com.order_service.event.order;


import com.order_service.event.base.BaseEvent;
import lombok.Getter;
import lombok.experimental.SuperBuilder;


@Getter
@SuperBuilder
public abstract class OrderEvent extends BaseEvent {
}
