package com.plural.sight.practical;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import scala.xml.PrettyPrinter;

public class Words {

    public static void main(String[] args) throws Exception {

        final ParameterTool tool = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        environment.getConfig().setGlobalJobParameters(tool);

        DataStream<String> dataStream = null;

        if (tool.has("input")) {
            System.out.println(" Using INPUT FILE ");
            // read input
            dataStream = environment.readTextFile(tool.get("input"));
        } else if (tool.has("host") && tool.has("port")) {
            System.out.println(" Using SOCKET INPUT ");
            dataStream = environment.socketTextStream(tool.get("host"), Integer.parseInt(tool.get("port")));
        }

        dataStream.flatMap(new Splitter()).print();

        environment.execute();

    }

    private static class Splitter implements FlatMapFunction<String, String> {
        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            String[] splits = value.split("\\W+");
            for (String split : splits)
                out.collect(split);
        }
    }

}
