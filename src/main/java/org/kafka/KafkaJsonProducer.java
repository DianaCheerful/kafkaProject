package org.kafka;

import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;


import java.util.Properties;
import java.util.UUID;


public class KafkaJsonProducer {

    public static void main(String[] args) throws Exception {
        // Настройки для подключения к Kafka и Schema Registry
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, "3");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(KafkaJsonSchemaSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        // Создание Kafka Producer
        Producer<String, String> producer = new KafkaProducer<>(props);

        // Отправка сообщения в Kafka
        ProducerRecord<String, String> record =
                new ProducerRecord<>("myTopic11", UUID.randomUUID().toString(), "test message9");
        ProducerRecord<String, String> record2 =
                new ProducerRecord<>("myTopic11", UUID.randomUUID().toString(), "test message10");
        producer.send(record).get();
        producer.send(record2).get();

        // Закрытие Producer
        producer.close();
    }
}
