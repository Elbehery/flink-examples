package com.plural.sight.understanding.batch;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class Top10Movies {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<Tuple2<Long, Double>> ratings = environment.readCsvFile("/home/mustafa/Documents/github/BigData/ApacheFlink/FlinkMoocExamples/src/main/resources/data/ratings.csv")
                .ignoreFirstLine()
                .ignoreInvalidLines()
                .includeFields(false, true, true, false)
                .types(Long.class, Double.class);

        DataSet<Tuple2<Long, Double>> topTenRatings = ratings.groupBy(0).reduceGroup(new AverageMovieRating())
                .partitionCustom(new RatingPartitioner(), 1)
                .setParallelism(6)
                .sortPartition(1, Order.DESCENDING)
                .mapPartition(new Top10MapPartition())
                .sortPartition(1, Order.DESCENDING)
                .setParallelism(1)
                .mapPartition(new Top10MapPartition());

        DataSource<Tuple2<Long, String>> movies = environment.readCsvFile("/home/mustafa/Documents/github/BigData/ApacheFlink/FlinkMoocExamples/src/main/resources/data/movies.csv")
                .ignoreFirstLine()
                .ignoreInvalidLines()
                .parseQuotedStrings('"')
                .includeFields(true, true, false)
                .types(Long.class, String.class);

        topTenRatings.join(movies)
                .where(0).equalTo(0)
                .with(new JoinRatingWithMovie())
                .print();

    }

    private static class AverageMovieRating implements GroupReduceFunction<Tuple2<Long, Double>, Tuple2<Long, Double>> {

        @Override
        public void reduce(Iterable<Tuple2<Long, Double>> values, Collector<Tuple2<Long, Double>> out) throws Exception {

            double sum = 0.0;
            int counter = 0;
            long movieId = 0;

            for (Tuple2<Long, Double> item : values) {
                movieId = item.f0;
                sum += item.f1;
                counter++;
            }

            double average = sum / counter;

            out.collect(new Tuple2<>(movieId, average));
        }
    }

    private static class RatingPartitioner implements Partitioner<Double> {
        @Override
        public int partition(Double key, int numPartitions) {
            return key.intValue();
        }
    }

    private static class Top10MapPartition implements MapPartitionFunction<Tuple2<Long, Double>, Tuple2<Long, Double>> {

        @Override
        public void mapPartition(Iterable<Tuple2<Long, Double>> values, Collector<Tuple2<Long, Double>> out) throws Exception {

            Iterator<Tuple2<Long, Double>> valueIterator = values.iterator();
            for (int i = 0; i < 10 && valueIterator.hasNext(); i++) {
                out.collect(valueIterator.next());
            }
        }
    }

    private static class JoinRatingWithMovie implements JoinFunction<Tuple2<Long, Double>, Tuple2<Long, String>, Tuple3<Long, String, Double>> {

        @Override
        public Tuple3<Long, String, Double> join(Tuple2<Long, Double> first, Tuple2<Long, String> second) throws Exception {
            return new Tuple3<>(first.f0, second.f1, first.f1);
        }
    }
}