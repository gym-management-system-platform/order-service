package com.order_service.event.inventory;


import lombok.Getter;
import lombok.experimental.SuperBuilder;


@Getter
@SuperBuilder
public class InventoryReserveFailedEvent extends InventoryEvent {

    private final String reason;
}