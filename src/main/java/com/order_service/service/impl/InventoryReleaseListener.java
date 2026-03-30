package com.order_service.service.impl;


import com.order_service.event.inventory.InventoryReleasedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;


@Component
@RequiredArgsConstructor
@Slf4j
public class InventoryReleaseListener {

    private final OrderServiceImpl orderServiceImpl;

    @KafkaListener(
            topics = "${app.kafka.topics.inventory-released:inventory-released}",
            groupId = "order-service-group",
            containerFactory = "inventoryReleasedKafkaListenerContainerFactory"
    )
    public Mono<Void> handleInventoryReleased(InventoryReleasedEvent event) {
        if (event == null) {
            return Mono.empty();
        }
        return orderServiceImpl.cancelAfterCompensation(event.getSagaId())
                .then()
                .onErrorResume(ex -> {
                    log.error("Failed to process inventory release", ex);
                    return Mono.empty();
                });
    }
}
