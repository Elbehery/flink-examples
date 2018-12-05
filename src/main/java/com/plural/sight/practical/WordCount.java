package com.plural.sight.practical;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCount {

    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.getConfig().setGlobalJobParameters(parameterTool);

        DataStream<String> dataStream = StreamUtil.getDataStream(executionEnvironment, parameterTool);

        if (dataStream == null)
            System.exit(1);

        dataStream.flatMap(new Splitter()).keyBy(0).sum(1).print();

        executionEnvironment.execute("WordCount");
    }

    private static class Splitter implements FlatMapFunction<String, Tuple2<String, Long>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {

            String[] parts = value.split("\\W+");
            for (String part : parts)
                out.collect(new Tuple2<String, Long>(part, 1L));

        }
    }
}
