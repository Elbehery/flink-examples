package com.plural.sight.practical;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class CarSpeed {

    public static void main(String[] args) throws Exception {

        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.getConfig().setGlobalJobParameters(parameterTool);

        DataStream<String> dataStream = StreamUtil.getDataStream(executionEnvironment, parameterTool);

        if (dataStream == null)
            System.exit(1);

        dataStream.map(new SpeedMapper()).keyBy(0).flatMap(new SpeedLimitDetector()).print();

        executionEnvironment.execute("CarSpeed");
    }

    private static class SpeedMapper implements MapFunction<String, Tuple2<Integer, Double>> {

        @Override
        public Tuple2<Integer, Double> map(String value) throws Exception {
            return new Tuple2<>(1, Double.parseDouble(value));
        }
    }

    private static class SpeedLimitDetector extends RichFlatMapFunction<Tuple2<Integer, Double>, String> {

        private transient ValueState<Tuple2<Integer, Double>> countSumState;

        @Override
        public void flatMap(Tuple2<Integer, Double> input, Collector<String> out) throws Exception {

            Tuple2<Integer, Double> currentCountSumState = countSumState.value();

            if (input.f1 > 65.0) {

                out.collect(String.format("Achtung !!!!! ... Your speed is %s, the average speed of the last %s cars is %s ", input.f1, currentCountSumState.f0, currentCountSumState.f1 / currentCountSumState.f0));

                countSumState.clear();
                currentCountSumState = countSumState.value();

            } else
                out.collect("GOOD TO GO");


            currentCountSumState.f0 += 1;
            currentCountSumState.f1 += input.f1;

            countSumState.update(currentCountSumState);
        }

        @Override
        public void open(Configuration parameters) throws Exception {

            ValueStateDescriptor<Tuple2<Integer, Double>> valueStateDescriptor = new ValueStateDescriptor<Tuple2<Integer, Double>>("speedLimitState", TypeInformation.of(new TypeHint<Tuple2<Integer, Double>>() {
            }), Tuple2.of(0, 0.0));

            countSumState = getRuntimeContext().getState(valueStateDescriptor);
        }
    }
}
