package com.plural.sight.understanding.batch;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.StringValue;
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
                // TODO : BON REMARK
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
    private static class MovieGenreRatingTransformer implements JoinFunction<Tuple3<Long, String, String>, Tuple2<Long, Double>, Tuple3<StringValue, StringValue, DoubleValue>> {

        private StringValue name = new StringValue();
        private StringValue genre = new StringValue();
        private DoubleValue score = new DoubleValue();
        private Tuple3<StringValue, StringValue, DoubleValue> result = new Tuple3<>(name, genre, score);

        @Override
        public Tuple3<StringValue, StringValue, DoubleValue> join(Tuple3<Long, String, String> movie, Tuple2<Long, Double> rating) throws Exception {

            name.setValue(movie.f1);
            genre.setValue(movie.f2.split("\\|")[0]);
            score.setValue(rating.f1);
            return result;
        }
    }

    // GroupingBy
    private static class MovieGenreGroupDistribution implements GroupReduceFunction<Tuple3<StringValue, StringValue, DoubleValue>, Tuple2<String, Double>> {

        @Override
        public void reduce(Iterable<Tuple3<StringValue, StringValue, DoubleValue>> values, Collector<Tuple2<String, Double>> out) throws Exception {

            double totalSum = 0;
            int counter = 0;
            String genre = null;

            for (Tuple3<StringValue, StringValue, DoubleValue> tuple : values) {

                genre = tuple.f1.getValue();
                totalSum += tuple.f2.getValue();
                counter++;
            }

            out.collect(new Tuple2<>(genre, totalSum / counter));
        }
    }
}
