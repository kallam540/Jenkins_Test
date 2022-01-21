package com.nokia;

import example.avro.MM;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AMFStreamJob {
    public static void main(String[] args) throws Exception {
        // create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

     /* Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");
        properties.put("group.id", "customer-consumer-group-v1");
        properties.put("auto.commit.enable", "false");
        properties.put("auto.offset.reset", "earliest");

        // avro part (deserializer)
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", SimpleAvroSchemaJava.class.getName());
        properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");
        properties.setProperty("specific.avro.reader", "true");*//*



        //FlinkKafkaConsumer<User> flinkkafkaconsumer = new FlinkKafkaConsumer<User>("inputGMFCSV2", new SimpleAvroSchemaFlink(), properties);
        //flinkkafkaconsumer.setStartFromEarliest(); */


        KafkaSource<MM> source = KafkaSource.<MM>builder()
                .setBootstrapServers("127.0.0.1:9092")
                .setTopics("inputGMFCSV6")
                .setGroupId("customer-consumer-group-v1")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleAvroSchemaFlink())
                .build();


        DataStream<MM> stream =   env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source"); //env.addSource(flinkkafkaconsumer); //

        System.out.println("TEst log");
        System.out.println(stream.print());

        stream.print();


        KafkaSink<MM> sink = KafkaSink.<MM>builder()
                .setBootstrapServers("127.0.0.1:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("outputGMFCSV")
                        .setValueSerializationSchema(new SimpleAvroSchemaFlink())
                        .build()
                )
                .build();

        //stream.sinkTo(sink);

       stream.map(new MapFunction<MM, MM>() {
            @Override
            public MM map(MM userBehavior) throws Exception {
                userBehavior.setIMSI(userBehavior.getIMSI()+ " Name Updated on 20 01");
                userBehavior.setFavoriteNumber(userBehavior.getFavoriteNumber()+ " Number Updated on 20 01");
                userBehavior.setFavoriteColor(" Color Updated on 20 01");
                System.out.print("return values");
                return userBehavior;
            }
        }).sinkTo(sink);

        stream.print();

        env.execute("ReadFromKafka");
    }
}
