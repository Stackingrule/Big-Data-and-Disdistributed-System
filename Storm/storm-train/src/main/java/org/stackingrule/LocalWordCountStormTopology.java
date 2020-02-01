package org.stackingrule;

import org.apache.commons.io.FileUtils;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * 使用Storm完成词频统计
 */
public class LocalWordCountStormTopology {


    public static class DataSourceSpout extends BaseRichSpout {

        private SpoutOutputCollector collector;
        @Override
        public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {
            this.collector = collector;

        }

        /*
        * 读取指定目录文件夹下的数据
        * 把数据发射出处
         */
        @Override
        public void nextTuple() {

            Collection<File> files = FileUtils.listFiles(new File("E:\\word"),
                    new String[]{"txt"}, true);
            for (File file : files) {
                try {
                    List<String> lines = FileUtils.readLines(file);
                    for (String line : lines) {
                        this.collector.emit(new Values(line));
                    }

                    // TODO
                    FileUtils.moveFile(file, new File(file.getAbsolutePath() + System.currentTimeMillis()));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

            outputFieldsDeclarer.declare(new Fields("line"));
        }
    }

    public static class SplitBolt extends BaseRichBolt {

        private OutputCollector collector;
        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {

            String line = tuple.getStringByField("line");
            String[] words = line.split(",");

            for (String word : words) {
                this.collector.emit(new Values(word));
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

            outputFieldsDeclarer.declare(new Fields("word"));
        }
    }

    public static class CountBolt extends BaseRichBolt {

        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        }

        Map<String, Integer> map = new HashMap<>();

        @Override
        public void execute(Tuple tuple) {

            String word = tuple.getStringByField("word");
            Integer count = map.get(word);
            if (count == null) {
                count = 0;
            } else {
                count ++;
            }

            map.put(word, count);

            System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~");
            Set<Map.Entry<String, Integer>> entrySet = map.entrySet();
            for (Map.Entry<String, Integer> entry : entrySet) {
                System.out.println(entry);
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        }
    }

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("DataSourceSpout", new DataSourceSpout());
        builder.setBolt("SplitBolt", new SplitBolt()).shuffleGrouping("DataSourceSpout");
        builder.setBolt("CountBolt", new CountBolt()).shuffleGrouping("SplitBolt");

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("LocalSumStormTopology", new Config(),
                builder.createTopology());
    }

}
