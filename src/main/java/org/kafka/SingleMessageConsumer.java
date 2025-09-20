package org.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class SingleMessageConsumer {
    public static void main(String[] args) {
        // Настройка консьюмера
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");  // Адрес брокера Kafka
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group-single");        // Уникальный идентификатор группы
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");        // Начало чтения с самого начала
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");           // Автоматический коммит смещений
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);           // Макс. количество записей за 1 poll
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "6000");           // Время ожидания активности от консьюмера

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Подписка на топик

        // Чтение сообщений в бесконечном цикле
        try (consumer) {
            consumer.subscribe(Collections.singletonList("myTopic11"));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));  // Получение сообщений
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Получено сообщение: key = %s, value = %s, partition = %d, offset = %d%n",
                            record.key(), record.value(), record.partition(), record.offset());
                }
            }
        } catch (Exception e) {
            System.out.printf("error: " + e);
        }
    }
}