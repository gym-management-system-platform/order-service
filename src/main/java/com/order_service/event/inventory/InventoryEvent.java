package com.order_service.event.inventory;


import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.order_service.event.base.BaseEvent;
import lombok.Getter;
import lombok.experimental.SuperBuilder;

@JsonTypeInfo(use = JsonTypeInfo.Id.DEDUCTION)
@JsonSubTypes({
        @JsonSubTypes.Type(InventoryReservedEvent.class),
        @JsonSubTypes.Type(InventoryReserveFailedEvent.class)
})
@Getter
@SuperBuilder
public abstract class InventoryEvent extends BaseEvent {
    private final String productId;
    private final Integer quantity;
}
