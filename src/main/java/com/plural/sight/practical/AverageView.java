package com.plural.sight.practical;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AverageView {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        environment.getConfig().setGlobalJobParameters(parameterTool);

        DataStream<String> inputStream = StreamUtil.getDataStream(environment, parameterTool);

        if (inputStream == null)
            System.exit(1);

        inputStream.map(new RawPageSplitter()).keyBy(0).reduce(new SumTimeAndCountReducer()).keyBy(0).map(new AverageTimeMapper()).print();
        environment.execute();
    }

    private static class RawPageSplitter implements MapFunction<String, Tuple3<String, Double, Long>> {

        @Override
        public Tuple3<String, Double, Long> map(String value) throws Exception {

            String[] parts = value.split(",");
            if (parts.length == 2)
                return new Tuple3<String, Double, Long>(parts[0], Double.parseDouble(parts[1]), 1L);

            return null;
        }

    }

    private static class SumTimeAndCountReducer implements ReduceFunction<Tuple3<String, Double, Long>> {
        @Override
        public Tuple3<String, Double, Long> reduce(Tuple3<String, Double, Long> value1, Tuple3<String, Double, Long> value2) throws Exception {

            return (new Tuple3<>(value1.f0, value1.f1 + value2.f1, value1.f2 + value2.f2));
        }
    }

    private static class AverageTimeMapper implements MapFunction<Tuple3<String, Double, Long>, Tuple2<String, Double>> {

        @Override
        public Tuple2<String, Double> map(Tuple3<String, Double, Long> value) throws Exception {
            return new Tuple2<>(value.f0, value.f1 / value.f2);
        }
    }
}
