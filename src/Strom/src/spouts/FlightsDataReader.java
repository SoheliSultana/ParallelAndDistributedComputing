package spouts;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import javax.swing.text.html.HTMLDocument;

public class FlightsDataReader extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private FileReader fileReader;
    private boolean completed = false;

    public void ack(Object msgId) {
        System.out.println("OK:" + msgId);
    }

    public void close() {
    }

    public void fail(Object msgId) {
        System.out.println("FAIL:" + msgId);
    }

    /**
     * The only thing that the methods will do It is emit each
     * file line
     */
    public void nextTuple() {
        /**
         * The nextuple it is called forever, so if we have been readed the file
         * we will wait and then return
         */
        if (completed) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                //Do nothing
            }
            return;
        }
        JSONParser parser = new JSONParser();

        try {
            // read file as in Json format
            Object obj = parser.parse(fileReader);
            JSONObject jsonObject = (JSONObject) obj;
            JSONArray flighstList = (JSONArray) jsonObject.get("states");
            //Read all flight's data and invoke flightNormalization method
            // to get 17 fields of flight
            for (Object flight : flighstList) {
                JSONArray flightInfo = (JSONArray) flight;
                Object[] objects = flightNormalizer(flightInfo.iterator());
                String callsign = (String) objects[1];

                if (objects == null || objects.length == 0 || objects.length < 17) {
                    continue;
                }
                ;

                // By each flight emmit a new value with the line 17 fields as object
                this.collector.emit(new Values(objects));
            }
        } catch (Exception e) {
            throw new RuntimeException("Error reading tuple", e);
        } finally {
            completed = true;
        }
    }

    /**
     * We will create the file and get the collector object
     */
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        try {
            this.fileReader = new FileReader(conf.get("FlightsFile").toString());
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Error reading file [" + conf.get("FlightsFile") + "]");
        }
        this.collector = collector;
    }

    /**
     * Declare the output field with 17 fields of flight information
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("transponder address",
                "call sign", "origin country", "last timestamp", "last timesstamp", "longitude",
                "latitude", "atlitude(barometric)", "surface or air", "velocity(meters/sec)",
                "dergree north = 0", "vertical rate", "sensors", "atlitude(geometric)",
                "transpondar code", "special purpose", "origin"));
    }

    /**
     * This method invokes for each flights data normalization
     *
     * @param flightDetails
     * @return
     */
    Object[] flightNormalizer(Iterator<Object> flightDetails) {
        List<Object> list = new ArrayList<Object>();
        int i=0;
        while (flightDetails.hasNext()) {
            if(i==11){
               Object verticalRate = flightDetails.next();
                if(verticalRate instanceof Long){
                    double value = 1.0 * (Long)verticalRate;
                    list.add(Double.valueOf(value));
                }else{
                    list.add(verticalRate);
                }
            }else {
                list.add(flightDetails.next());
            }
             i++;
        }
        return list.toArray();
    }

}
