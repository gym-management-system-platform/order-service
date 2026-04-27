package com.order_service.config;


import com.fitness.kafka.KafkaListenerContainerFactoryBuilder;
import com.fitness.kafka.KafkaProperties;
import com.order_service.event.inventory.InventoryEvent;
import com.order_service.event.inventory.InventoryReleasedEvent;
import com.order_service.event.payment.PaymentFailedEvent;
import com.order_service.event.payment.PaymentSucceededEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;


@Configuration
public class KafkaConfig {

    private final KafkaListenerContainerFactoryBuilder listenerFactoryBuilder;
    private final KafkaProperties kafkaProperties;

    public KafkaConfig(KafkaListenerContainerFactoryBuilder listenerFactoryBuilder, KafkaProperties kafkaProperties) {
        this.listenerFactoryBuilder = listenerFactoryBuilder;
        this.kafkaProperties = kafkaProperties;
    }

    @Bean
    public OrderOutboxTopicResolver orderOutboxTopicResolver() {
        return new OrderOutboxTopicResolver(kafkaProperties);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, InventoryEvent> inventoryEventsKafkaListenerContainerFactory() {
        return listenerFactoryBuilder.json(InventoryEvent.class);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, PaymentSucceededEvent> paymentSucceededKafkaListenerContainerFactory() {
        return listenerFactoryBuilder.json(PaymentSucceededEvent.class);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, PaymentFailedEvent> paymentFailedKafkaListenerContainerFactory() {
        return listenerFactoryBuilder.json(PaymentFailedEvent.class);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, InventoryReleasedEvent> inventoryReleasedKafkaListenerContainerFactory() {
        return listenerFactoryBuilder.json(InventoryReleasedEvent.class);
    }
}
