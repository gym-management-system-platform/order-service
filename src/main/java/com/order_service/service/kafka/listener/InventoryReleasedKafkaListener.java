package com.order_service.service.kafka.listener;

import com.order_service.event.inventory.InventoryReleasedEvent;
import com.order_service.service.kafka.OrderKafkaInboundApplicationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class InventoryReleasedKafkaListener {

    private final OrderKafkaInboundApplicationService orderKafkaInbound;

    @KafkaListener(
            topics = "${app.kafka.listener.inventory-released.topic}",
            groupId = "${app.kafka.listener.inventory-released.group-id}",
            containerFactory = "${app.kafka.listener.inventory-released.container-factory}",
            batch = "${app.kafka.listener.inventory-released.batch-mode}",
            concurrency = "${app.kafka.listener.inventory-released.concurrency}"
    )
    public void handle(InventoryReleasedEvent event, Acknowledgment ack) {
        if (event == null) {
            ack.acknowledge();
            return;
        }
        orderKafkaInbound.onInventoryReleased(event)
                .doFinally(signalType -> ack.acknowledge())
                .subscribe(
                        v -> log.debug("Обработано inventory-released, sagaId={}", event.getSagaId()),
                        error -> log.error("Ошибка обработки inventory-released, sagaId={}", event.getSagaId(), error)
                );
    }
}
