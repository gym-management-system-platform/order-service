package com.order_service.event.inventory;


import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;


public enum InventoryWireEventType {
    INVENTORY_RESERVED(InventoryReservedEvent.class),
    INVENTORY_RESERVE_FAILED(InventoryReserveFailedEvent.class),
    INVENTORY_RELEASED(InventoryReleasedEvent.class);

    private static final Map<Class<? extends InventoryEvent>, InventoryWireEventType> BY_INVENTORY_EVENT_CLASS =
            Collections.unmodifiableMap(
                    Arrays.stream(values()).collect(Collectors.toMap(InventoryWireEventType::inventoryEventClass, e -> e)));

    private final Class<? extends InventoryEvent> inventoryEventClass;

    InventoryWireEventType(Class<? extends InventoryEvent> inventoryEventClass) {
        this.inventoryEventClass = inventoryEventClass;
    }

    public Class<? extends InventoryEvent> inventoryEventClass() {
        return inventoryEventClass;
    }

    public static InventoryWireEventType fromJsonTypeId(String id) {
        try {
            return valueOf(id);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Неизвестный идентификатор типа события инвентаря: " + id, e);
        }
    }

    public static InventoryWireEventType forConcreteInventoryEvent(Class<?> clazz) {
        InventoryWireEventType type = BY_INVENTORY_EVENT_CLASS.get(clazz);
        if (type == null) {
            throw new IllegalArgumentException("Неизвестный класс InventoryEvent: " + clazz.getName());
        }
        return type;
    }
}
