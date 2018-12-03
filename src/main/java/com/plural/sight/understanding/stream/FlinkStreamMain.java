package com.plural.sight.understanding.stream;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkStreamMain {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Integer> integerStream = streamExecutionEnvironment.fromElements(4, 5, 6, 7, 8, 9);

        integerStream.filter(new IntegerStreamFilter()).print();

        streamExecutionEnvironment.execute();

    }

    private static class IntegerStreamFilter implements FilterFunction<Integer> {

        @Override
        public boolean filter(Integer value) throws Exception {
            return value % 2 == 0;
        }
    }
}
