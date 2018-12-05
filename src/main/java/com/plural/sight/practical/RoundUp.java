package com.plural.sight.practical;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RoundUp {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> dataStream = environment.socketTextStream("localhost", 9999);

        dataStream.filter(new FilterString()).map(new RoundUpMapper()).print();

        environment.execute("RoundUp");

    }


    private static class FilterString implements FilterFunction<String> {

        @Override
        public boolean filter(String input) throws Exception {

            try {

                Double.parseDouble(input);
                return true;

            } catch (Exception e) {
            }

            return false;
        }
    }

    private static class RoundUpMapper implements MapFunction<String, Long> {

        @Override
        public Long map(String value) throws Exception {
            return Math.round(Double.parseDouble(value));
        }
    }
}
