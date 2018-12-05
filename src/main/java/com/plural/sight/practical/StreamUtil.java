package com.plural.sight.practical;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamUtil {

    public static DataStream<String> getDataStream(StreamExecutionEnvironment environment, ParameterTool parameterTool) {

        DataStream<String> dataStream = null;

        if (parameterTool.has("input")) {
            System.out.println(" Using INPUT FILE ");
            dataStream = environment.readTextFile(parameterTool.get("input"));

        } else if (parameterTool.has("host") && parameterTool.has("port")) {
            System.out.println(" Using SOCKET INPUT ");
            dataStream = environment.socketTextStream(parameterTool.get("host"), Integer.parseInt(parameterTool.get("port")));

        }

        return dataStream;
    }
}