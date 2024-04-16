package net.bondarik;

import net.bondarik.producer.SimpleStringProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.Future;

public class App 
{
    public static void main( String[] args ) {
        KafkaProducer producer = SimpleStringProducer.buildProducer();
        ProducerRecord record = SimpleStringProducer.buildRecord();
        Future<RecordMetadata> result1 = producer.send(record);
        //synchronized send -> result1.get();

        Future<RecordMetadata> result2 = producer.send(record, (recordMetadata, e) -> callback());

    }

    public static void callback() {
        System.out.println("Callback invoke!");
    }
}
