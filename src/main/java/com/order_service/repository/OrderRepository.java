package com.order_service.repository;


import com.order_service.entity.OrderEntity;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

import java.util.UUID;


@Repository
public interface OrderRepository extends ReactiveCrudRepository<OrderEntity, UUID> {

    Mono<OrderEntity> findBySagaId(UUID sagaId);

}
