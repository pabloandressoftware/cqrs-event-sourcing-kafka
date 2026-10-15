package com.techbank.account.cmd.infrastructure;

import com.techbank.cqrs.core.events.BaseEvent;
import com.techbank.cqrs.core.producers.EventProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class AccountEventProducer implements EventProducer {
    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Override
    public void produce(String topic, BaseEvent event) {
        // The aggregate id is used as the record key so every event of the same account
        // lands in the same partition and keeps its order even if the topic has several partitions
        this.kafkaTemplate.send(topic, event.getId(), event);
    }
}
