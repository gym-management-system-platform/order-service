package com.order_service.service.kafka.listener;

import com.order_service.event.payment.PaymentSucceededEvent;
import com.order_service.service.kafka.OrderKafkaInboundApplicationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentSucceededKafkaListener {

    private final OrderKafkaInboundApplicationService orderKafkaInbound;

    @KafkaListener(
            topics = "${app.kafka.topics.payment-succeeded:payment-succeeded}",
            groupId = "order-service-group",
            containerFactory = "paymentSucceededKafkaListenerContainerFactory"
    )
    public void handle(PaymentSucceededEvent event, Acknowledgment ack) {
        if (event == null) {
            ack.acknowledge();
            return;
        }
        orderKafkaInbound.onPaymentSucceeded(event)
                .doFinally(signalType -> ack.acknowledge())
                .subscribe(
                        v -> log.debug("Обработано payment-succeeded, sagaId={}", event.getSagaId()),
                        error -> log.error("Ошибка обработки payment-succeeded, sagaId={}", event.getSagaId(), error)
                );
    }
}
