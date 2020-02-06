package com.stackingrule.flink.java.course02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;



/**
 *  使用Java开发Flink的批处理应用程序
 */
public class BatchWCJavaApp {




    public static void main(String[] args) throws Exception {

        String input = "file:///D:/work/tmp/flink/input";

        /**
         * 1. set up the batch execution environment
         *
         */
        // ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        /**
         * 2. read data 读数据
         */
        DataSource<String> text = env.readTextFile(input);

        /**
         * 3. tranform
         */

        text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] tokens = value.toLowerCase().split("\\W+");

                for (String token : tokens) {
                    if (token.length() > 0) {
                        collector.collect(new Tuple2<String, Integer>(token, 1));
                    }
                }
            }
        }).groupBy(0).sum(1).print();
    }

}
