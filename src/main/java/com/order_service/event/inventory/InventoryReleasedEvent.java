package com.order_service.event.inventory;


import lombok.Getter;
import lombok.experimental.SuperBuilder;


@Getter
@SuperBuilder
public class InventoryReleasedEvent extends InventoryEvent {
}
