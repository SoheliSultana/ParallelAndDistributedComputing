package bolts;

import java.util.*;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class AirlineSorter extends BaseBasicBolt {

    Integer id;
    String name;
    Map<String, Map<String, Integer>> counters;
    Map<String, String> cityCodeMap;
    Map<String, Map<String, String>> flightdir;

    /**
     * At the end of the spout (when the cluster is shutdown
     * We will show the airport city, closet flights for that
     * that airport code with counters
     */
    @Override
    public void cleanup() {
        System.out.println("-- Flight Counter [" + name + "-" + id + "] --");

        Set<Map.Entry<String, Map<String, Integer>>> entries = counters.entrySet();


        for (Map.Entry<String, Map<String, Integer>> entry : entries) {
            int totalFlight = 0;
            String airportCode = entry.getKey();
            System.out.println("At airport: " + airportCode + "(" + cityCodeMap.get(airportCode) + ")");

            Map<String, Integer> map = entry.getValue();
            Map<String, String> dir = flightdir.get(airportCode);
            // printing the flight direction whether taking off or departing
            for (String callsing : dir.keySet()) {
                String[] str = dir.get(callsing).split(" ");
                System.out.println("  " + str[0] + " : " + str[1]);
            }

            // print flight with count, original portion of code
            for (String flight : map.keySet()) {
                //System.out.println("  " + flight + ": " + map.get(flight));
                totalFlight = totalFlight + map.get(flight);
            }

            // additional features, sorted the airlines in decsending order
			/*for (Map.Entry<String, Integer> e: sortFlightList(map)) {
				System.out.println( "  " + e.getKey() +": "+ e.getValue());
				totalFlight = totalFlight + e.getValue();
			}*/
            System.out.println("  #total flight = " + totalFlight);
        }

    }

    /**
     * On create
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        this.counters = new HashMap<String, Map<String, Integer>>();
        this.name = context.getThisComponentId();
        this.id = context.getThisTaskId();
        this.cityCodeMap = new HashMap<String, String>();
        this.flightdir = new HashMap<String, Map<String, String>>();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    /**
     * This bolt will receive input Tuple from previous bolt
     * that contains airport city, code  and closet flight of that flight
     *
     * @param input
     * @param collector
     */

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String city = input.getStringByField("airport city");
        String airportCode = input.getStringByField("airport code");
        String callSign = input.getStringByField("call sign");
        double vertical_rate = input.getDoubleByField("vertical rate");

        cityCodeMap.put(airportCode, city);

        if (!flightdir.containsKey(airportCode)) {
            flightdir.put(airportCode, new HashMap<String, String>());
        }
        if (vertical_rate < 0) {
            flightdir.get(airportCode).put(callSign.substring(0, 3), callSign + " " + "Landing");
        } else {
            flightdir.get(airportCode).put(callSign.substring(0, 3), callSign + " " + "Departing");
        }

        if (callSign.length() < 3) {
            return;
        }
        callSign = callSign.substring(0, 3);


        /**
         * If the airport code doesn't exist in the map we will create
         * this, if not We will create a new map that is <call sign, count></>
         * and add 1 for that call sign
         * If the airprt code exist, it will check the key, call sign
         * If call sign exist it will increase the value by 1
         * otherwise put the call sign with count 1
         */
        if (!counters.containsKey(airportCode)) {
            Map<String, Integer> map = new HashMap<String, Integer>();
            map.put(callSign, 1);
            counters.put(airportCode, map);
        } else {

            Map<String, Integer> airport = counters.get(airportCode);
            if (airport.containsKey(callSign)) {
                airport.put(callSign, airport.get(callSign) + 1);
            } else {
                airport.put(callSign, 1);
            }

        }
    }

    /**
     * This method return the sorted flight list in descending order
     *
     * @param map
     * @return
     */
    List<Map.Entry<String, Integer>> sortFlightList(Map<String, Integer> map) {
        List<Map.Entry<String, Integer>> entries = new ArrayList<Map.Entry<String, Integer>>(map.entrySet());
        Collections.sort(entries, new Comparator<Map.Entry<String, Integer>>() {
            @Override
            public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                return o2.getValue() - o1.getValue();
            }
        });
        return entries;
    }
}
