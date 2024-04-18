package net.bondarik.consumer;

import com.sun.tools.javac.util.List;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Несколько потребителей, относящихся к одной группе, не могут работать в одном потоке,
 * и несколько потоков не могут безопасно использовать один и тот же потребитель.
 * Железное правило: один потребитель на один поток.
 */

public class SimpleStringConsumer {

    public KafkaConsumer<String, String> buildConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer_group_id");
        //props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "some_id");//static member
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());


        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");//earliest, latest, none

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");//управляем фиксацией смещения вручную

        return new KafkaConsumer<>(props);
    }


    public void startConsumer() {
        KafkaConsumer<String, String> consumer = buildConsumer();
        consumer.subscribe(List.of("topic_1", "topic_2"));
        consumer.subscribe(Pattern.compile("test.*")); // regexp регулярно перечитывает все топики и подписывается на подходящие


        Duration timeout = Duration.ofMillis(100);
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(timeout); //постоянный опрос, таймаут ожидания ответа

            //обрабатываем батч сообщений
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("topic = %s, partition = %d, offset = %d, customer = %s, country = %s\n",
                                  record.topic(),
                                  record.partition(),
                                  record.offset(),
                                  record.key(),
                                  record.value());

                record.headers();
                //+ обработка и кеширование
            }

            try {
                consumer.commitSync();
            } catch (CommitFailedException e) {
                System.out.println("log error");
                e.printStackTrace();
            }
        }
    }
}
