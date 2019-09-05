package com.geekymv.storm.helloworld;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.rmi.runtime.Log;

import java.util.Map;

public class TwoBolt extends BaseBasicBolt {

    private static final Logger LOG = LoggerFactory.getLogger(TwoBolt.class);

    private TopologyContext context;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        this.context = context;
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String word = input.getStringByField("word");
        LOG.info("two bolt word = " + word);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void cleanup() {
        LOG.info("TwoBolt cleanup method");
    }
}
