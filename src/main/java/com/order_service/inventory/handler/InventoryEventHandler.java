package com.order_service.inventory.handler;


import com.order_service.event.inventory.InventoryEvent;
import reactor.core.publisher.Mono;


public interface InventoryEventHandler {

    boolean supports(InventoryEvent event);

    Mono<Void> handle(InventoryEvent event);
}
