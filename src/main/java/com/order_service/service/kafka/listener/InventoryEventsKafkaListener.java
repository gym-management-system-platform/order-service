package com.order_service.service.kafka.listener;


import com.order_service.event.inventory.InventoryEvent;
import com.order_service.service.kafka.OrderKafkaInboundApplicationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.util.List;


@Component
@RequiredArgsConstructor
@Slf4j
public class InventoryEventsKafkaListener {

    private final OrderKafkaInboundApplicationService orderKafkaInbound;

    @KafkaListener(
            topics = "${app.kafka.listener.inventory-events.topic}",
            groupId = "${app.kafka.listener.inventory-events.group-id}",
            containerFactory = "${app.kafka.listener.inventory-events.container-factory}",
            batch = "${app.kafka.listener.inventory-events.batch-mode}",
            concurrency = "${app.kafka.listener.inventory-events.concurrency}"
    )
    public void handle(List<InventoryEvent> events, Acknowledgment ack) {
        if (events == null || events.isEmpty()) {
            ack.acknowledge();
            return;
        }
        Flux.fromIterable(events)
                .concatMap(orderKafkaInbound::onInventoryEvent)
                .doFinally(signalType -> ack.acknowledge())
                .subscribe(
                        v -> log.debug("Обработан batch inventory-events, size={}", events.size()),
                        error -> log.error("Ошибка batch inventory-events, size={}", events.size(), error)
                );
    }
}
