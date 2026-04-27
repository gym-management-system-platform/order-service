package com.order_service.inventory.handler;


import com.order_service.enums.OrderStatus;
import com.order_service.event.inventory.InventoryEvent;
import com.order_service.event.inventory.InventoryReserveFailedEvent;
import com.order_service.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;


@Component
@RequiredArgsConstructor
@Slf4j
public class InventoryReserveFailedEventHandler implements InventoryEventHandler {

    private final OrderRepository repository;
    private final TransactionalOperator transactionalOperator;

    @Override
    public boolean supports(InventoryEvent event) {
        return event instanceof InventoryReserveFailedEvent;
    }

    @Override
    public Mono<Void> handle(InventoryEvent event) {
        InventoryReserveFailedEvent failed = (InventoryReserveFailedEvent) event;
        return repository.findBySagaId(failed.getSagaId())
                .doOnNext(o -> log.info("Найден заказ {}", o.getId()))
                .doOnSuccess(o -> log.info("Результат: {}", o))
                .flatMap(order -> {
                    log.info("Обновление заказа {}", order.getId());
                    order.setStatus(OrderStatus.FAILED);
                    order.setFailureReason("Резерв инвентаря не выполнен: " + failed.getReason());
                    return repository.save(order);
                })
                .as(transactionalOperator::transactional)
                .then();
    }
}
