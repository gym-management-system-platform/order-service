package com.order_service.service.kafka.publisher;


import com.order_service.config.OrderOutboxTopicResolver;
import com.order_service.entity.OutboxEventEntity;
import com.order_service.enums.OutboxEventStatus;
import com.order_service.repository.OutboxRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;


@Service
@RequiredArgsConstructor
public class OrderOutboxPublishingService {

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafka;
    private final OrderOutboxTopicResolver orderOutboxTopicResolver;

    public Mono<Void> publishRow(OutboxEventEntity event) {
        try {
            if (event.getPayload() == null) {
                return markFailed(event, new IllegalStateException("Полезная нагрузка outbox отсутствует"));
            }
            String payload = event.getPayload().asString();
            if (payload.isBlank()) {
                return markFailed(event, new IllegalStateException("Полезная нагрузка outbox пуста"));
            }
            String topic = orderOutboxTopicResolver.resolve(event.getEventType());
            return Mono.fromFuture(kafka.send(topic, payload).toCompletableFuture())
                    .then(markSent(event))
                    .onErrorResume(e -> markFailed(event, e));
        } catch (Exception e) {
            return markFailed(event, e);
        }
    }

    private Mono<Void> markSent(OutboxEventEntity event) {
        event.setStatus(OutboxEventStatus.SENT);
        event.setProcessedAt(Instant.now());
        return outboxRepository.save(event).then();
    }

    private Mono<Void> markFailed(OutboxEventEntity event, Throwable error) {
        event.setStatus(OutboxEventStatus.FAILED);
        event.setLastError(error != null ? error.getMessage() : null);
        event.setRetryCount(event.getRetryCount() != null ? event.getRetryCount() + 1 : 1);
        return outboxRepository.save(event).then();
    }
}
