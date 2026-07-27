package com.order_service.service.kafka.listener;

import com.order_service.event.payment.PaymentFailedEvent;
import com.order_service.service.kafka.OrderKafkaInboundApplicationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentFailedKafkaListener {

    private final OrderKafkaInboundApplicationService orderKafkaInbound;

    @KafkaListener(
            topics = "${app.kafka.listener.payment-failed.topic}",
            groupId = "${app.kafka.listener.payment-failed.group-id}",
            containerFactory = "${app.kafka.listener.payment-failed.container-factory}",
            batch = "${app.kafka.listener.payment-failed.batch-mode}",
            concurrency = "${app.kafka.listener.payment-failed.concurrency}"
    )
    public void handle(PaymentFailedEvent event, Acknowledgment ack) {
        if (event == null) {
            ack.acknowledge();
            return;
        }
        orderKafkaInbound.onPaymentFailed(event)
                .doFinally(signalType -> ack.acknowledge())
                .subscribe(
                        v -> log.debug("Обработано payment-failed, sagaId={}", event.getSagaId()),
                        error -> log.error("Ошибка обработки payment-failed, sagaId={}", event.getSagaId(), error)
                );
    }
}
