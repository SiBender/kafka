package net.bondarik.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class SimpleStringProducer {
    public static KafkaProducer<String, String> buildProducer() {
        Properties kafkaProperties = new Properties();
        String serializerName = org.apache.kafka.common.serialization.StringSerializer.class.getName();
        kafkaProperties.put("bootstrap.servers", "broker1:9092,broker2:9092");
        kafkaProperties.put("key.serializer", serializerName);
        kafkaProperties.put("value.serializer", serializerName);

        kafkaProperties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, CountingProducerInterceptor.class.getName());

        //https://kafka.apache.org/documentation.html#producerconfigs
        kafkaProperties.put("client.id", "Microservice_id");
        kafkaProperties.put("acks", "all"); //all (-1), 0, 1
        kafkaProperties.put("max.block.ms", "100"); //wait for completion send() then throw Interrupted Exception
        kafkaProperties.put("deliver.timeout.ms", "120000"); //полный таймаут с пакетированием и ретраями - 120 сек
        kafkaProperties.put("request.timeout.ms", "1000"); //таймаут одной попытки отправки
        kafkaProperties.put("retries", "100"); //Количество авто ретраев
        kafkaProperties.put("linger.ms", "1000"); //Время накопления батча сообщений
        kafkaProperties.put("compression.type", "gzip");
        kafkaProperties.put("batch.size", "100000");//размер батча в байтах!

        //влияют на порядок записи
        kafkaProperties.put("max.in.flight.requests.per.connection", "5");
        kafkaProperties.put("enable.idempotence", "true");


        KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProperties);
        return producer;
    }

    public static ProducerRecord<String, String> buildRecord() {
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>("topic.Topic name", "Key.Precision Products", "Value.France");


        //add headers
        producerRecord.headers().add("property-name","property-value".getBytes(StandardCharsets.UTF_8));
        //value always byte[]

        return producerRecord;
    }
}
