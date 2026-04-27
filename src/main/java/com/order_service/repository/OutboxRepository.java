package com.order_service.repository;


import com.order_service.entity.OutboxEventEntity;
import com.order_service.enums.OutboxEventStatus;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;

import java.util.UUID;


@Repository
public interface OutboxRepository
        extends ReactiveCrudRepository<OutboxEventEntity, UUID> {

    Flux<OutboxEventEntity> findTop50ByStatusOrderByCreatedAtAsc(OutboxEventStatus status);
}

