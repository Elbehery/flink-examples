package com.plural.sight.understanding;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class FlinkMain {

    public static void main(String[] args) throws Exception {


        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> lines = env.fromElements(
                "Apache Flink is a community-driven open source framework for distributed big data analytics,",
                "like Hadoop and Spark. The core of Apache Flink is a distributed streaming dataflow engine written",
                " in Java and Scala.[1][2] It aims to bridge the gap between MapReduce-like systems and shared-nothing",
                "parallel database systems. Therefore, Flink executes arbitrary dataflow programs in a data-parallel and",
                "pipelined manner.[3] Flink's pipelined runtime system enables the execution of bulk/batch and stream",
                "processing programs.[4][5] Furthermore, Flink's runtime supports the execution of iterative algorithms natively.[6]"
        );

        lines.flatMap(new WordCounter())
//                .map(x -> new Tuple2<String, Integer>(x, 1))
                .map(new WordMapper())
                .groupBy(0)
                .sum(1)
                .print();


    }

    private static class WordCounter implements FlatMapFunction<String, String> {

        @Override
        public void flatMap(String s, Collector<String> collector) throws Exception {

            String[] splits = s.split("\\W+");
            for (String split : splits)
                collector.collect(split);
        }
    }

    private static class WordMapper implements MapFunction<String, Tuple2<String, Integer>> {

        @Override
        public Tuple2<String, Integer> map(String value) throws Exception {
            return new Tuple2<>(value, 1);
        }
    }
}
