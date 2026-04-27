package com.order_service.inventory.handler;


import com.order_service.enums.OrderStatus;
import com.order_service.event.inventory.InventoryEvent;
import com.order_service.event.inventory.InventoryReservedEvent;
import com.order_service.repository.OrderRepository;
import com.order_service.service.OrderService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;


@Component
@RequiredArgsConstructor
public class InventoryReservedEventHandler implements InventoryEventHandler {

    private final OrderRepository repository;
    private final TransactionalOperator transactionalOperator;
    private final OrderService orderService;

    @Override
    public boolean supports(InventoryEvent event) {
        return event instanceof InventoryReservedEvent;
    }

    @Override
    public Mono<Void> handle(InventoryEvent event) {
        InventoryReservedEvent reserved = (InventoryReservedEvent) event;
        return updateOrderForReserved(reserved)
                .then(orderService.startPayment(reserved));
    }

    private Mono<Void> updateOrderForReserved(InventoryReservedEvent event) {
        return repository.findBySagaId(event.getSagaId())
                .flatMap(order -> {
                    if (order.getStatus() == OrderStatus.COMPLETED
                            || order.getStatus() == OrderStatus.PROCESSING_PAYMENT) {
                        return Mono.empty();
                    }
                    order.setStatus(OrderStatus.PROCESSING_PAYMENT);
                    return repository.save(order).thenReturn(order);
                })
                .as(transactionalOperator::transactional)
                .then();
    }
}
