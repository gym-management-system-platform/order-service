package com.order_service.service;


import com.order_service.entity.OutboxEventEntity;
import com.order_service.enums.OutboxEventStatus;
import com.order_service.enums.OutboxEventType;
import com.order_service.event.OrderCompensatedEvent;
import com.order_service.event.OrderCreatedEvent;
import com.order_service.event.OrderProcessingPaymentEvent;
import com.order_service.repository.OutboxRepository;
import com.order_service.property.KafkaProperties;
import com.order_service.serialize.EventSerializer;
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
    private final KafkaTemplate<String, Object> kafka;
    private final EventSerializer eventSerializer;
    private final KafkaProperties kafkaProperties;


    @Scheduled(fixedDelayString = "${app.outbox.poll-interval-ms}")
    public void process() {
        outboxRepository.findTop50ByStatusOrderByCreatedAtAsc(OutboxEventStatus.NEW.name())
                .flatMap(this::publishEvent)
                .subscribe();
    }

    private Mono<Void> publishEvent(OutboxEventEntity event) {
        try {
            Object payload = deserialize(event);
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

    private Object deserialize(OutboxEventEntity event) {
        if (event.getEventType() == OutboxEventType.OrderCreatedEvent) {
            return eventSerializer.fromJson(event.getPayload(), OrderCreatedEvent.class);
        }
        if (event.getEventType() == OutboxEventType.OrderCompensatedEvent) {
            return eventSerializer.fromJson(event.getPayload(), OrderCompensatedEvent.class);
        }
        if (event.getEventType() == OutboxEventType.OrderProcessingPaymentEvent) {
            return eventSerializer.fromJson(event.getPayload(), OrderProcessingPaymentEvent.class);
        }
        throw new RuntimeException("Unknown event type: " + event.getEventType());
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