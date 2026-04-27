package com.order_service.inventory.handler;


import com.order_service.event.inventory.InventoryEvent;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Optional;


@Component
public class InventoryEventHandlerRegistry {

    private final List<InventoryEventHandler> handlers;

    public InventoryEventHandlerRegistry(List<InventoryEventHandler> handlers) {
        this.handlers = handlers;
    }


    public Optional<Mono<Void>> dispatch(InventoryEvent event) {
        return handlers.stream()
                .filter(h -> h.supports(event))
                .findFirst()
                .map(h -> h.handle(event));
    }
}
