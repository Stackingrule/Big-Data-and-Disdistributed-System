package com.stackingrule.flink.java.course02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.util.Collector;
import scala.Tuple2;


/**
 *  使用Java开发Flink的批处理应用程序
 */
public class BatchWCJavaApp {




    public static void main(String[] args) {
        /**
         * 1. set up the batch execution environment
         */
        String input = "file:///D:/work/tmp/flink/input";
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        /**
         * 2. read data 读数据
         */
        DataSource<String> text = env.readTextFile(input);

        /**
         * 3. tranform
         */

        text.flatMap(new FlatMapFunction<String, Tuple2<String, Object>>() {

            @Override
            public void flatMap(String s, Collector<Tuple2<String, Object>> collector) throws Exception {

            }
        });
    }

}
