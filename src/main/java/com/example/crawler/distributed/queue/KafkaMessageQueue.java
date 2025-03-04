package com.example.crawler.distributed.queue;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Kafka消息队列实现
 */
public class KafkaMessageQueue implements MessageQueue {
    private static final Logger logger = LoggerFactory.getLogger(KafkaMessageQueue.class);

    private final KafkaProducer<String, String> producer;
    private final ConcurrentHashMap<String, KafkaConsumer<String, String>> consumers;
    private final ConcurrentHashMap<String, ExecutorService> consumerThreads;
    private final AtomicBoolean running;
    private final String groupId;
    private final String bootstrapServers;

    public KafkaMessageQueue(String bootstrapServers, String groupId) {
        this.bootstrapServers = bootstrapServers;
        this.groupId = groupId;
        this.producer = createProducer();
        this.consumers = new ConcurrentHashMap<>();
        this.consumerThreads = new ConcurrentHashMap<>();
        this.running = new AtomicBoolean(true);
    }

    @Override
    public void send(String topic, Object message) {
        try {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(topic, message.toString());
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    logger.error("Error sending message to topic {}: {}",
                            topic, exception.getMessage());
                } else {
                    logger.debug("Message sent to topic {}, partition {}",
                            metadata.topic(), metadata.partition());
                }
            });
        } catch (Exception e) {
            logger.error("Failed to send message to topic {}: {}", topic, e.getMessage());
        }
    }

    @Override
    public void subscribe(String topic, MessageHandler handler) {
        if (consumers.containsKey(topic)) {
            logger.warn("Already subscribed to topic: {}", topic);
            return;
        }

        KafkaConsumer<String, String> consumer = createConsumer(topic);
        consumers.put(topic, consumer);

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        consumerThreads.put(topic, executorService);

        executorService.submit(() -> {
            try {
                while (running.get()) {
                    ConsumerRecords<String, String> records =
                            consumer.poll(Duration.ofMillis(100));
                    records.forEach(record -> {
                        try {
                            Message msg = new Message(topic, record.value(), null);
                            handler.handle(msg);
                        } catch (Exception e) {
                            logger.error("Error handling message from topic {}: {}",
                                    topic, e.getMessage());
                        }
                    });
                }
            } catch (Exception e) {
                logger.error("Error in consumer thread for topic {}: {}",
                        topic, e.getMessage());
            } finally {
                consumer.close();
            }
        });
    }

    @Override
    public void close() {
        running.set(false);

        // 关闭生产者
        if (producer != null) {
            producer.close();
        }

        // 关闭所有消费者线程
        consumerThreads.values().forEach(ExecutorService::shutdownNow);

        // 关闭所有消费者
        consumers.values().forEach(KafkaConsumer::close);

        consumers.clear();
        consumerThreads.clear();
    }

    private KafkaProducer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<>(props);
    }

    private KafkaConsumer<String, String> createConsumer(String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
        return consumer;
    }
}