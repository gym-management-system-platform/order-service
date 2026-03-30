package com.order_service.service.impl;


import com.order_service.entity.OutboxEventEntity;
import com.order_service.enums.OutboxEventStatus;
import com.order_service.enums.OutboxEventType;
import com.order_service.repository.OutboxRepository;
import com.order_service.config.property.KafkaProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Instant;


@Component
@RequiredArgsConstructor
@Slf4j
public class OutboxProcessor {

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafka;
    private final KafkaProperties kafkaProperties;

    @Scheduled(fixedDelayString = "${app.outbox.poll-interval-ms}")
    public void process() {
        outboxRepository.findTop50ByStatusOrderByCreatedAtAsc(OutboxEventStatus.NEW.name())
                .flatMap(this::publishEvent)
                .subscribe();
    }

    private Mono<Void> publishEvent(OutboxEventEntity event) {
        try {
            if (event.getPayload() == null) {
                return markFailed(event, new IllegalStateException("Outbox payload is null"));
            }
            String payload = event.getPayload().asString();
            if (payload.isBlank()) {
                return markFailed(event, new IllegalStateException("Outbox payload string is empty"));
            }
            String topic = resolveTopic(event.getEventType());
            return Mono.fromFuture(
                            kafka.send(topic, payload)
                                    .toCompletableFuture()
                    )
                    .then(markSent(event))
                    .onErrorResume(e -> markFailed(event, e));
        } catch (Exception e) {
            return markFailed(event, e);
        }
    }

    private String resolveTopic(OutboxEventType eventType) {
        String eventKey = switch (eventType) {
            case OrderCreatedEvent -> "order-created";
            case OrderCompensatedEvent -> "order-compensated";
            case OrderProcessingPaymentEvent -> "order-processing-payment";
        };

        String topic = kafkaProperties.getTopics().get(eventKey);
        if (topic == null || topic.isBlank()) {
            log.warn("Kafka topic for key '{}' is not configured, falling back to key name", eventKey);
            return eventKey;
        }
        return topic;
    }

    private Mono<Void> markSent(OutboxEventEntity event) {
        event.setStatus(OutboxEventStatus.SENT.name());
        event.setProcessedAt(Instant.now());
        return outboxRepository.save(event).then();
    }

    private Mono<Void> markFailed(OutboxEventEntity event, Throwable error) {
        event.setStatus(OutboxEventStatus.FAILED.name());
        event.setLastError(error != null ? error.getMessage() : null);
        event.setRetryCount(event.getRetryCount() != null ? event.getRetryCount() + 1 : 1);
        return outboxRepository.save(event).then();
    }

}