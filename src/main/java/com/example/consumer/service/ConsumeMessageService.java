package com.example.consumer.service;

import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
@Slf4j
public class ConsumeMessageService {

    // Opción 1: Manual ACK con Channel y DeliveryTag
    @RabbitListener(queues = "myQueue.bootcamp")
    public void consumeMessageManualAck (String message, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) {

        log.info ("Received message: {}", message);

        try {
            // Simular procesamiento del mensaje
            processMessage (message);

            // ACKNOWLEDGE MANUAL - Confirmar que el mensaje se procesó correctamente
            channel.basicAck (deliveryTag, false);
            log.info ("Message acknowledged successfully: {}", message);

        } catch (Exception e) {
            log.error ("Error processing message: {}", message, e);

            try {
                // REJECT - Rechazar el mensaje y mandarlo de vuelta a la cola
                channel.basicNack (deliveryTag, false, true); // requeue = true
                log.info ("Message rejected and requeued: {}", message);

                // O si no quieres que vuelva a la cola:
                // channel.basicNack(deliveryTag, false, false); // requeue = false

            } catch (IOException ioException) {
                log.error ("Error rejecting message", ioException);
            }
        }
    }

    // Opción 2: Manual ACK con objeto Message completo
    @RabbitListener(queues = "myQueue.bootcamp")
    public void consumeMessageWithFullMessage (Message message, Channel channel) {
        String body = new String (message.getBody ());
        long deliveryTag = message.getMessageProperties ()
                .getDeliveryTag ();

        log.info ("Received message: {}", body);

        try {
            processMessage (body);

            // ACK manual
            channel.basicAck (deliveryTag, false);
            log.info ("Message processed and acknowledged: {}", body);

        } catch (Exception e) {
            log.error ("Error processing message: {}", body, e);

            try {
                // NACK con requeue
                channel.basicNack (deliveryTag, false, true);
            } catch (IOException ioException) {
                log.error ("Error sending NACK", ioException);
            }
        }
    }

    private void processMessage (String message) throws Exception {
        // Simular procesamiento que puede fallar
        log.info ("Processing message: {}", message);

        // Simular trabajo
        Thread.sleep (1000);

        // Simular error ocasional (10% de probabilidad)
        if (Math.random () < 0.9) {
            throw new RuntimeException ("Simulated processing error");
        }

        log.info ("Message processed successfully: {}", message);
    }
}