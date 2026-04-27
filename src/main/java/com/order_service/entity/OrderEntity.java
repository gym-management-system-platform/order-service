package com.order_service.entity;


import com.order_service.enums.Currency;
import com.order_service.enums.OrderStatus;
import lombok.*;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.relational.core.mapping.Column;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;


@Table("orders")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OrderEntity {

    @Id
    @Column("id")
    private UUID id;

    @Column("order_number")
    private String orderNumber;

    @Column("user_id")
    private UUID userId;

    @Column("amount")
    private BigDecimal amount;

    @Column("product_id")
    private UUID productId;

    @Column("quantity")
    private Integer quantity;

    @Column("currency")
    private Currency currency = Currency.RUB;

    @Column("status")
    private OrderStatus status = OrderStatus.PENDING;

    @Column("failure_reason")
    private String failureReason;

    @CreatedDate
    @Column("created_at")
    private Instant createdAt;

    @LastModifiedDate
    @Column("updated_at")
    private Instant updatedAt;

    @Column("saga_id")
    private UUID sagaId;
}