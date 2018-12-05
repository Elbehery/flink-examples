package com.plural.sight.practical;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class CarSpeedReducedState {

    public static void main(String[] args) throws Exception {

        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.getConfig().setGlobalJobParameters(parameterTool);

        DataStream<String> dataStream = StreamUtil.getDataStream(executionEnvironment, parameterTool);

        if (dataStream == null)
            System.exit(1);

        dataStream.map(new SpeedMapper()).keyBy(0).flatMap(new SpeedLimitReducerDetector()).print();

        executionEnvironment.execute("CarSpeed");
    }

    private static class SpeedMapper implements MapFunction<String, Tuple2<Integer, Double>> {

        @Override
        public Tuple2<Integer, Double> map(String value) throws Exception {
            return new Tuple2<>(1, Double.parseDouble(value));
        }
    }

    private static class SpeedLimitReducerDetector extends RichFlatMapFunction<Tuple2<Integer, Double>, String> {

        private transient ValueState<Integer> countState;
        private transient ReducingState<Double> sumSpeedState;

        @Override
        public void flatMap(Tuple2<Integer, Double> input, Collector<String> out) throws Exception {

            if (input.f1 > 65.0) {

                int count = countState.value();
                double sum = sumSpeedState.get();

                out.collect(String.format("Achtung !!!!! ... Your speed is %s, the average speed of the last %s cars is %s ", input.f1, count, sum / count));

                countState.clear();
                sumSpeedState.clear();

            } else
                out.collect("GOOD TO GO");


            countState.update(countState.value() + 1);
            sumSpeedState.add(input.f1);

        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Integer> countStateValueDescriptor = new ValueStateDescriptor<Integer>("CarCounter", Integer.class, 0);
            countState = getRuntimeContext().getState(countStateValueDescriptor);

            ReducingStateDescriptor<Double> sumState = new ReducingStateDescriptor<Double>("CarSpeed", new ReduceFunction<Double>() {
                @Override
                public Double reduce(Double value1, Double value2) throws Exception {
                    return value1 + value2;
                }
            }, Double.class);

            sumSpeedState = getRuntimeContext().getReducingState(sumState);
        }
    }

}
