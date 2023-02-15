package io.redpanda.examples;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TradePipeline {

    final static String inputTopic = "orders-topic";
    final static String outputTopic = "trade-topic";
    final static String jobTitle = "TradePipeline";

    public static void main(String[] args) throws Exception {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:19092";

        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<TradeVO> source = KafkaSource.<TradeVO>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(inputTopic)
                .setGroupId("trade-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new TradeDeserializationSchema())
                .build();

        KafkaRecordSerializationSchema<TradeVO> serializer = KafkaRecordSerializationSchema.builder()
                .setValueSerializationSchema(new TradeSerializationSchema())
                .setTopic(outputTopic)
                .build();

        KafkaSink<TradeVO> sink = KafkaSink.<TradeVO>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(serializer)
                .build();

        DataStream<TradeVO> inputTrades = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Trade Source");

        DataStream<TradeVO> outputTrades = inputTrades.filter(new FilterFunction<TradeVO>() {
            @Override
            public boolean filter(TradeVO tradeVO) throws Exception {
                return "FIXED".equalsIgnoreCase(tradeVO.getTicketType());
            }
        });


        // Split up the lines in pairs (2-tuples) containing: (word,1)
        /**
        DataStream<String> counts = text.flatMap(new WordCount.Tokenizer())
                // Group by the tuple field "0" and sum up tuple field "1"
                .keyBy(value -> value.f0)
                .sum(1)
                .flatMap(new Reducer());
         */

        // Add the sink to so results
        // are written to the outputTopic
        outputTrades.sinkTo(sink);

        // Execute program
        env.execute(jobTitle);
    }
}
