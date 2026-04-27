package com.order_service.service.kafka.listener;


import com.order_service.event.inventory.InventoryEvent;
import com.order_service.service.kafka.OrderKafkaInboundApplicationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;


@Component
@RequiredArgsConstructor
@Slf4j
public class InventoryEventsKafkaListener {

    private final OrderKafkaInboundApplicationService orderKafkaInbound;

    @KafkaListener(
            topics = "${app.kafka.topics.inventory-events:inventory-events}",
            groupId = "order-service-group",
            containerFactory = "inventoryEventsKafkaListenerContainerFactory"
    )
    public void handle(InventoryEvent event, Acknowledgment ack) {
        if (event == null) {
            ack.acknowledge();
            return;
        }
        orderKafkaInbound.onInventoryEvent(event)
                .doFinally(signalType -> ack.acknowledge())
                .subscribe(
                        v -> log.debug("Обработано событие инвентаря, sagaId={}", event.getSagaId()),
                        error -> log.error("Ошибка обработки события инвентаря, sagaId={}", event.getSagaId(), error)
                );
    }
}
