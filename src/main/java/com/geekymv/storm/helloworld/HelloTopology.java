package com.geekymv.storm.helloworld;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

public class HelloTopology {

    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("helloSpout", new HelloSpout());

        builder.setBolt("oneBolt", new OneBolt()).shuffleGrouping("helloSpout");
        builder.setBolt("twoBolt", new TwoBolt()).shuffleGrouping("helloSpout");

        Config conf = new Config();
        conf.setNumWorkers(4);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("helloTopology", conf, builder.createTopology());

        Utils.sleep(100_000);

        cluster.shutdown();
    }

}
