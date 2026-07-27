package com.order_service.service.kafka.listener;


import com.order_service.event.payment.PaymentSucceededEvent;
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
public class PaymentSucceededKafkaListener {

    private final OrderKafkaInboundApplicationService orderKafkaInbound;

    @KafkaListener(
            topics = "${app.kafka.listener.payment-succeeded.topic}",
            groupId = "${app.kafka.listener.payment-succeeded.group-id}",
            containerFactory = "${app.kafka.listener.payment-succeeded.container-factory}",
            batch = "${app.kafka.listener.payment-succeeded.batch-mode}",
            concurrency = "${app.kafka.listener.payment-succeeded.concurrency}"
    )
    public void handle(List<PaymentSucceededEvent> events, Acknowledgment ack) {
        if (events == null || events.isEmpty()) {
            ack.acknowledge();
            return;
        }
        Flux.fromIterable(events)
                .concatMap(orderKafkaInbound::onPaymentSucceeded)
                .doFinally(signalType -> ack.acknowledge())
                .subscribe(
                        v -> log.debug("Обработан batch payment-succeeded, size={}", events.size()),
                        error -> log.error("Ошибка batch payment-succeeded, size={}", events.size(), error)
                );
    }
}
