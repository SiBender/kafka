package net.bondarik.producer;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public class CountingProducerInterceptor implements ProducerInterceptor<String, String> {
    private static final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    private static final LongAdder numSent = new LongAdder();
    private static final AtomicLong numAcked = new AtomicLong(0);


    public void configure(Map<String, ?> map) {
        /*Long windowSize = Long.valueOf(
                (String) map.get("counting.interceptor.window.size.ms"));*/
        Long windowSize = 500L;
        executorService.scheduleAtFixedRate(CountingProducerInterceptor::run,
                windowSize, windowSize, TimeUnit.MILLISECONDS);
    }
    public ProducerRecord onSend(ProducerRecord producerRecord) {
        numSent.increment();
        return producerRecord;
    }
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        numAcked.incrementAndGet();
    }
    public void close() {
        executorService.shutdownNow();
    }
    public static void run() {
        System.out.println(numSent.sumThenReset());
        System.out.println(numAcked.getAndSet(0));
    }
}