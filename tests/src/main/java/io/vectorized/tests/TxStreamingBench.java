package io.vectorized.tests;

import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.function.Function;
import java.lang.Math;
import java.util.Properties;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class TxStreamingBench extends Consts 
{
    String connection;

    String source_topic;
    TopicPartition source_tp;
    List<TopicPartition> source_tps;

    String target_topic;

    Producer<String, String> producer;
    Consumer<String, String> consumer;
    String groupId;

    public TxStreamingBench(String connection, String source_topic, String target_topic) {
        this.connection = connection;
        this.source_topic = source_topic;
        this.source_tp = new TopicPartition(source_topic, 0);
        this.source_tps = Collections.singletonList(new TopicPartition(source_topic, 0));

        this.target_topic = target_topic;
    }

    public void initProducer(String txId) throws Exception {
        Properties pprops = new Properties();
        pprops.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, connection);
        pprops.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        pprops.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, txId);
        pprops.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        pprops.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        this.producer = new KafkaProducer<>(pprops);
        this.producer.initTransactions();
    }

    public void initConsumer(String groupId) {
        var isolation = "read_committed";

        Properties cprops = new Properties();
        cprops.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, connection);
        cprops.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        cprops.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        cprops.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolation);
        cprops.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        cprops.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        cprops.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        this.groupId = groupId;
        this.consumer = new KafkaConsumer<>(cprops);
        this.consumer.subscribe(Collections.singleton(source_topic));
    }

    long fillSource(int iterations) throws Exception {
        Properties pprops = new Properties();
        pprops.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, connection);
        pprops.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        pprops.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        pprops.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        this.producer = new KafkaProducer<>(pprops);

        long offset = producer.send(new ProducerRecord<String, String>(source_topic, "key0", "value0")).get().offset();
        for (int i=1;i<iterations;i++) {
            producer.send(new ProducerRecord<String, String>(source_topic, "key"+i, "value"+i));
        }
        this.producer.close();
        return offset;
    }

    public void setGroupStartOffset(long start) throws Exception {
        Map<TopicPartition, OffsetAndMetadata> offsets;

        producer.beginTransaction();
        offsets = new HashMap<>();
        offsets.put(source_tp, new OffsetAndMetadata(start));
        producer.sendOffsetsToTransaction(offsets, groupId);
        producer.commitTransaction();
    }

    void warmup(int iterations, int batchSize) throws Exception {
        int count = 0;
        while (count < iterations) {
            ConsumerRecords<String, String> records = consumer.poll(batchSize);
            var it = records.iterator();
            while (it.hasNext()) {
                count++;

                var record = it.next();

                String key = record.key();
                String value = record.value();
                
                producer.beginTransaction();
                producer.send(new ProducerRecord<String, String>(target_topic, key, value.toUpperCase()));
                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                offsets.put(source_tp, new OffsetAndMetadata(record.offset() + 1));
                producer.sendOffsetsToTransaction(offsets, groupId);
                producer.commitTransaction();
                
                if (count >= iterations) {
                    break;
                }
            }
        }
    }

    void measure(int iterations, int batchSize) throws Exception {
        int count = 0;
        
        long[] measures = new long[iterations];
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        long started = System.nanoTime();
        while (count < iterations) {
            ConsumerRecords<String, String> records = consumer.poll(batchSize);
            var it = records.iterator();
            while (it.hasNext()) {
                var record = it.next();

                String key = record.key();
                String value = record.value();
                
                long tx_started = System.nanoTime();
                producer.beginTransaction();
                producer.send(new ProducerRecord<String, String>(target_topic, key, value.toUpperCase()));
                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                offsets.put(source_tp, new OffsetAndMetadata(record.offset() + 1));
                producer.sendOffsetsToTransaction(offsets, groupId);
                producer.commitTransaction();
                long tx_elapsed = System.nanoTime() - tx_started;
                min = Math.min(min, tx_elapsed);
                max = Math.max(max, tx_elapsed);
                measures[count] = tx_elapsed;
                
                count++;
                if (count >= iterations) {
                    break;
                }
            }
        }
        long elapsed = System.nanoTime() - started;
        System.out.println("measured " + iterations + " txes in " + elapsed + "ns");
        Arrays.sort(measures);
        System.out.println("min: " + min + "ns");
        System.out.println("p50: " + measures[measures.length / 2] + "ns");
        System.out.println("p99: " + measures[(int)(measures.length * 0.99)] + "ns");
        System.out.println("max: " + max + "ns");

        BufferedWriter writer = new BufferedWriter(new FileWriter(getMeasuresFileName()));
        for (int i=0;i<measures.length;i++) {
            writer.write("" + measures[i] + "\n");
        }
        writer.close();
    }

    public static void main( String[] args ) throws Exception
    {
        var bench = new TxStreamingBench(getConnection(), "topic1", "topic2");
        long offset = bench.fillSource(2*getTxes(200));
        bench.initProducer("my-tx-1");
        
        bench.initConsumer("groupId");
        bench.setGroupStartOffset(offset);
        bench.warmup(10, 10);
        bench.consumer.close();
        
        bench.initConsumer("groupId");
        bench.setGroupStartOffset(offset);
        bench.measure(getTxes(200), 10);
        bench.consumer.close();
    }
}
