package com.example.consumer.config;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.RabbitListenerContainerFactory;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableRabbit
public class RabbitMQConfig {

    public static final String QUEUE_NAME = "myQueue.bootcamp";
    public static final String EXCHANGE_NAME = "exchange.direct.bootcamp";
    public static final String ROUTING_KEY = "myRoutingKey.bootcamp";

    // Asegurar que la cola existe (opcional si ya la creaste)
    @Bean
    Queue createQueue () {
        return QueueBuilder.durable (QUEUE_NAME)
                .build ();
    }

    @Bean
    DirectExchange exchange () {
        return ExchangeBuilder.directExchange (EXCHANGE_NAME)
                .durable (true)
                .build ();
    }

    @Bean
    Binding binding (Queue queue, DirectExchange exchange) {
        return BindingBuilder.bind (queue)
                .to (exchange)
                .with (ROUTING_KEY);
    }

    // Configurar el convertidor de mensajes para manejar Strings
    @Bean
    public MessageConverter messageConverter () {
        return new SimpleMessageConverter (); // Para strings simples
        // return new Jackson2JsonMessageConverter(); // Para JSON
    }

    @Bean
    public RabbitListenerContainerFactory <?> rabbitListenerContainerFactory(
            ConnectionFactory connectionFactory) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setMessageConverter(new SimpleMessageConverter());

        // CONFIGURACIÓN DE ACKNOWLEDGE
        factory.setAcknowledgeMode(AcknowledgeMode.MANUAL); // Manual ACK
        // factory.setAcknowledgeMode(AcknowledgeMode.AUTO); // Auto ACK (por defecto)
        // factory.setAcknowledgeMode(AcknowledgeMode.NONE); // Sin ACK

        // Configuraciones adicionales
        factory.setConcurrentConsumers(1);
        factory.setMaxConcurrentConsumers(5);
        factory.setPrefetchCount(1); // Un mensaje a la vez

        return factory;
    }
}