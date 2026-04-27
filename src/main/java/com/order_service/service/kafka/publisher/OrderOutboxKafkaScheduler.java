package com.order_service.service.kafka.publisher;

import com.order_service.enums.OutboxEventStatus;
import com.order_service.repository.OutboxRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class OrderOutboxKafkaScheduler {

    private final OutboxRepository outboxRepository;
    private final OrderOutboxPublishingService orderOutboxPublishingService;

    @Scheduled(fixedDelayString = "${app.outbox.poll-interval-ms}")
    public void pollAndPublish() {
        outboxRepository.findTop50ByStatusOrderByCreatedAtAsc(OutboxEventStatus.NEW)
                .flatMap(orderOutboxPublishingService::publishRow)
                .subscribe();
    }
}
