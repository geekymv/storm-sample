package com.geekymv.storm.helloworld;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OneBolt extends BaseBasicBolt {

    private static final Logger LOG = LoggerFactory.getLogger(OneBolt.class);

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String word = input.getStringByField("word");
        LOG.info("one bolt word = " + word);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
