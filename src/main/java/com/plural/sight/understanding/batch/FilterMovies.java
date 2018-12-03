package com.plural.sight.understanding.batch;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class FilterMovies {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<Tuple3<Long, String, String>> movieDataSource = env.readCsvFile("/home/mustafa/Documents/github/BigData/ApacheFlink/FlinkMoocExamples/src/main/resources/data/movies.csv")
                .ignoreFirstLine()
                .parseQuotedStrings('"')
                .ignoreInvalidLines()
                .types(Long.class, String.class, String.class);

        movieDataSource.map(new MovieMapper()).filter(new DramaFilter()).print();
    }


    // FilterFunction
    private static class DramaFilter implements FilterFunction<Movie> {

        private final static String FILTER_CRITERIA = "Drama";

        @Override
        public boolean filter(Movie value) throws Exception {
            return value.getGenres().contains(FILTER_CRITERIA);
        }
    }

    // MapFunction
    private static class MovieMapper implements MapFunction<Tuple3<Long, String, String>, Movie> {

        private Movie movie;

        @Override
        public Movie map(Tuple3<Long, String, String> value) throws Exception {

            movie = new Movie(value.f0, value.f1, parseStringIntoGenresSet(value.f2));
            return movie;
        }

        private Set<String> parseStringIntoGenresSet(String genres) {

            String[] genresNames = genres.split("\\|");

            return new HashSet<>(Arrays.asList(genresNames));
        }
    }

    public static class Movie {

        private Long movieId;
        private String titles;
        private Set<String> genres;

        public Movie() {
        }

        public Movie(Long movieId, String titles, Set<String> genres) {
            this.movieId = movieId;
            this.titles = titles;
            this.genres = genres;
        }

        public Long getMovieId() {
            return movieId;
        }

        public void setMovieId(Long movieId) {
            this.movieId = movieId;
        }

        public String getTitles() {
            return titles;
        }

        public void setTitles(String titles) {
            this.titles = titles;
        }

        public Set<String> getGenres() {
            return genres;
        }

        public void setGenres(Set<String> genres) {
            this.genres = genres;
        }

        @Override
        public String toString() {
            return "Movie{" +
                    "movieId=" + movieId +
                    ", titles='" + titles + '\'' +
                    ", genres=" + genres +
                    '}';
        }
    }
}
