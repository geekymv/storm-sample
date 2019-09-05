package com.geekymv.storm.helloworld;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class HelloSpout extends BaseRichSpout {

    private static final Logger LOG = LoggerFactory.getLogger(HelloSpout.class);

    private TopologyContext context;

    private SpoutOutputCollector collector;

    private Random random;

    private static final List<String> words = Arrays.asList("one", "two", "three", "four");

    @Override
    public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {
        this.context = context;
        this.collector = collector;
        this.random = new Random();

        LOG.info("HelloSpout open method, hashCode = {}, threadId = {}, taskId = {}"
                , this.hashCode(), Thread.currentThread().getId(), context.getThisTaskId());
    }

    @Override
    public void nextTuple() {
        Utils.sleep(100);
        collector.emit(new Values(words.get(random.nextInt(words.size()))));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}
