package com.order_service.event.inventory;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.DatabindContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.jsontype.impl.TypeIdResolverBase;

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;


public class InventoryEventTypeIdResolver extends TypeIdResolverBase {

    @Override
    public JsonTypeInfo.Id getMechanism() {
        return JsonTypeInfo.Id.CUSTOM;
    }

    @Override
    public String idFromValue(Object value) {
        if (value instanceof InventoryEvent event) {
            InventoryWireEventType type = event.getEventType();
            return Objects.requireNonNullElseGet(type, () -> InventoryWireEventType.forConcreteInventoryEvent(event.getClass())).name();
        }
        throw new IllegalStateException("Ожидался InventoryEvent, получен " + value.getClass().getName());
    }

    @Override
    public String idFromValueAndType(Object value, Class<?> suggestedType) {
        return idFromValue(value);
    }

    @Override
    public JavaType typeFromId(DatabindContext context, String id) {
        InventoryWireEventType type = InventoryWireEventType.fromJsonTypeId(id);
        return context.getTypeFactory().constructType(type.inventoryEventClass());
    }

    @Override
    public String getDescForKnownTypeIds() {
        return Arrays.stream(InventoryWireEventType.values()).map(Enum::name).collect(Collectors.joining(", "));
    }
}
