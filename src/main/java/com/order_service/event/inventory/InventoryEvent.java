package com.order_service.event.inventory;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;
import com.order_service.event.base.BaseEvent;
import lombok.Getter;
import lombok.experimental.SuperBuilder;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.CUSTOM,
        include = JsonTypeInfo.As.PROPERTY,
        property = "eventType",
        visible = true
)
@JsonTypeIdResolver(InventoryEventTypeIdResolver.class)
@JsonIgnoreProperties(ignoreUnknown = true)
@Getter
@SuperBuilder
public abstract class InventoryEvent extends BaseEvent {

    /**
     * Тип события в JSON из inventory-service ({@code visible = true} — Jackson заполняет поле при чтении).
     */
    private final InventoryWireEventType eventType;
}
