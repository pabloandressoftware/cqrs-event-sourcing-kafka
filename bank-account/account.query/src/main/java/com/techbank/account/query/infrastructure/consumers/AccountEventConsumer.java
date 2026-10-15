package com.techbank.account.query.infrastructure.consumers;

import com.techbank.account.query.infrastructure.handlers.EventHandler;
import com.techbank.cqrs.core.events.BaseEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.text.MessageFormat;
import java.util.logging.Logger;

@Service
public class AccountEventConsumer implements EventConsumer {
    private final Logger logger = Logger.getLogger(AccountEventConsumer.class.getName());

    @Autowired
    private EventHandler eventHandler;

    // A single listener on a single topic: events are consumed one by one, in the exact order
    // they were appended to the partition, regardless of their concrete type.
    @KafkaListener(topics = "${spring.kafka.topic}", groupId = "${spring.kafka.consumer.group-id}")
    @Override
    public void consume(@Payload BaseEvent event, Acknowledgment ack) {
        logger.info(MessageFormat.format("Consuming {0} (version {1}) for account {2}",
                event.getClass().getSimpleName(), event.getVersion(), event.getId()));
        try {
            // The concrete event type (sent in the __TypeId__ header by JsonSerializer) selects the on(...) overload
            var eventHandlerMethod = eventHandler.getClass().getDeclaredMethod("on", event.getClass());
            eventHandlerMethod.setAccessible(true);
            eventHandlerMethod.invoke(eventHandler, event);
            ack.acknowledge();
        } catch (Exception e) {
            throw new RuntimeException("Error while consuming event: " + event.getClass().getName(), e);
        }
    }
}
