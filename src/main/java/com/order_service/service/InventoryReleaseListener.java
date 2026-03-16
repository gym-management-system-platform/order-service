package com.order_service.service;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.order_service.event.InventoryReleasedEvent;
import com.order_service.property.KafkaProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;


@Component
@RequiredArgsConstructor
@Slf4j
public class InventoryReleaseListener {

    private final OrderService orderService;
    private final ObjectMapper objectMapper;
    private final KafkaProperties kafkaProperties;

    @KafkaListener(
            topics = "${app.kafka.topics.inventory-released:inventory-released}",
            groupId = "order-service-group"
    )
    public Mono<Void> handleInventoryReleased(String message) {

        return Mono.fromCallable(() ->
                        objectMapper.readValue(message, InventoryReleasedEvent.class)
                )
                .flatMap(event -> orderService.cancelAfterCompensation(event.sagaId()))
                .then()
                .onErrorResume(ex -> {
                    log.error("Failed to process inventory release", ex);
                    return Mono.empty();
                });
    }
}