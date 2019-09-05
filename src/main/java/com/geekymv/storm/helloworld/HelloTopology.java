package com.geekymv.storm.helloworld;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

public class HelloTopology {

    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("helloSpout", new HelloSpout(), 3);

        // 配置Executor 数量
        builder.setBolt("oneBolt", new OneBolt(), 2).shuffleGrouping("helloSpout");
        // 配置Task 数量 .setNumTasks(3)
        builder.setBolt("twoBolt", new TwoBolt(), 2).shuffleGrouping("helloSpout");

        Config conf = new Config();
        // 配置Worker 数量
        conf.setNumWorkers(1);

//        LocalCluster cluster = new LocalCluster();
//        cluster.submitTopology("helloTopology", conf, builder.createTopology());
//
//        Utils.sleep(100_000);
//
//        cluster.shutdown();

        try {
            StormSubmitter.submitTopology("helloTopology", conf, builder.createTopology());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
