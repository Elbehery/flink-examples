package com.plural.sight.understanding.batch;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class GenreRating {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<Tuple3<Long, String, String>> movieDataSource = env.readCsvFile("/home/mustafa/Documents/github/BigData/ApacheFlink/FlinkMoocExamples/src/main/resources/data/movies.csv")
                .ignoreFirstLine()
                .parseQuotedStrings('"')
                .ignoreInvalidLines()
                .types(Long.class, String.class, String.class);

        DataSource<Tuple2<Long, Double>> movieRatingsDataSource = env.readCsvFile("/home/mustafa/Documents/github/BigData/ApacheFlink/FlinkMoocExamples/src/main/resources/data/ratings.csv")
                .ignoreFirstLine()
                .parseQuotedStrings('"')
                .ignoreInvalidLines()
                .includeFields(false, true, true, false)
                .types(Long.class, Double.class);


        movieDataSource.join(movieRatingsDataSource)
                .where(0).equalTo(0)
                .with(new MovieGenreRatingTransformer())
                .groupBy(1)
                .reduceGroup(new MovieGenreGroupDistribution())
                .print();

    }

    // Join Transformation
    private static class MovieGenreRatingTransformer implements JoinFunction<Tuple3<Long, String, String>, Tuple2<Long, Double>, Tuple3<String, String, Double>> {

        @Override
        public Tuple3<String, String, Double> join(Tuple3<Long, String, String> movie, Tuple2<Long, Double> rating) throws Exception {

            String movieName = movie.f1;
            String movieGenre = movie.f2.split("\\|")[0];

            return new Tuple3<String, String, Double>(movieName, movieGenre, rating.f1);
        }
    }

    // GroupingBy
    private static class MovieGenreGroupDistribution implements GroupReduceFunction<Tuple3<String, String, Double>, Tuple2<String, Double>> {

        @Override
        public void reduce(Iterable<Tuple3<String, String, Double>> values, Collector<Tuple2<String, Double>> out) throws Exception {

            double totalSum = 0;
            int counter = 0;
            String genre = null;

            for (Tuple3<String, String, Double> tuple : values) {

                genre = tuple.f1;
                totalSum += tuple.f2;
                counter++;
            }

            out.collect(new Tuple2<>(genre, totalSum / counter));
        }
    }
}
