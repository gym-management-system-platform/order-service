package com.order_service.event.inventory;

import lombok.Getter;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

import java.util.UUID;

@Jacksonized
@Getter
@SuperBuilder
public class InventoryReservedEvent extends InventoryEvent {

    private final UUID productId;
    private final Integer quantity;
}