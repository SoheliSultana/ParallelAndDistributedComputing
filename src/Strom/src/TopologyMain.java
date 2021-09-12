import bolts.AirlineSorter;
import bolts.HubIdentifier;
import spouts.FlightsDataReader;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class TopologyMain {
    public static void main(String[] args) throws InterruptedException {

        //Topology definition
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("flight-reader", new FlightsDataReader());
        builder.setBolt("hub-identifier", new HubIdentifier())
                .shuffleGrouping("flight-reader");
        builder.setBolt("airline-sortner", new AirlineSorter(), 1)
                .fieldsGrouping("hub-identifier", new Fields("airport city"));

        //Configuration
        Config conf = new Config();
        conf.put("FlightsFile", args[0]);
        conf.put("AirportsData", args[1]);
        conf.setDebug(false);
        //Topology run
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Getting-Started-Toplogie", conf, builder.createTopology());
        Thread.sleep(1000);
        cluster.shutdown();
    }
}
