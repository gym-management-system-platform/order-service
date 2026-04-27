package com.order_service.entity;


import com.order_service.enums.OutboxAggregateType;
import com.order_service.enums.OutboxEventStatus;
import com.order_service.enums.OutboxEventType;
import io.r2dbc.postgresql.codec.Json;
import lombok.*;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import java.time.Instant;
import java.util.UUID;


@Table("outbox_event")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OutboxEventEntity {

    @Id
    @Column("id")
    private UUID id;

    @Column("aggregate_type")
    private OutboxAggregateType aggregateType;

    @Column("aggregate_id")
    private UUID aggregateId;

    @Column("event_type")
    private OutboxEventType eventType;

    @Column("payload")
    private Json payload;

    @Column("saga_id")
    private UUID sagaId;

    @Column("status")
    private OutboxEventStatus status;

    @CreatedDate
    @Column("created_at")
    private Instant createdAt;

    @Column("processed_at")
    private Instant processedAt;

    @Column("retry_count")
    private Integer retryCount;

    @Column("last_error")
    private String lastError;
}