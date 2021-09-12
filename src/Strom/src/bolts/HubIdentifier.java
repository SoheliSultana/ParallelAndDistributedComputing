package bolts;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.commons.lang.StringUtils;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.Map;

public class HubIdentifier extends BaseBasicBolt {

    private String[] airports;
    private FileReader fileReader;

    public void prepare(Map stormConf, TopologyContext contex) {
        String filename = stormConf.get("AirportsData").toString();
        try {
            readAirportData(filename);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void cleanup() {
    }

    /**
     * The bolt will receive flight information from the
     * spout and get Airport Data from Airport file
     * <p>
     * and emits those flights information who are closet to airport
     */

    public void execute(Tuple input, BasicOutputCollector collector) {
        String callSign = input.getStringByField("call sign").trim();

        double latitudeFlight = 0.0;
        double longitudeFlight = 0.0;
        double verticalRate = 0.0;
        try {
            // read latitude and longitude from Tuple by field
            latitudeFlight = input.getDoubleByField("latitude");
            longitudeFlight = input.getDoubleByField("longitude");
            verticalRate = input.getDoubleByField("vertical rate");
        } catch (Exception ex) {
            //ex.printStackTrace();
            return;
        }

        for (String airport : airports) {
            String[] words = airport.split(",");
            double latitudeAirport = Double.parseDouble(words[2].trim());
            double longitudeAirport = Double.parseDouble(words[3].trim());

            // distance calculation from degree to miles
            double latDis = distanceCalculation(latitudeAirport, latitudeFlight) * 70.0;
            double longDis = distanceCalculation(longitudeAirport, longitudeFlight) * 45.0;

            // ignore call signs which are empty
            if (callSign == null || callSign == " " || callSign.length() < 3) {
                return;
            }
            // emit to next bolt if both latitude and longitude of flight are
            // closest to airtport, between 20 miles
            if (latDis < 20.0 && longDis < 20.0) {
                String airportCity = words[0].trim();
                String airportCode = words[1].trim();

                if (verticalRate == 0.0) {
                    return;
                } else if (verticalRate < 0.0) {
                    collector.emit(new Values(airportCity, airportCode, callSign, verticalRate));
                } else {
                    collector.emit(new Values(airportCity, airportCode, callSign, verticalRate));
                }

            }
        }

    }

    /**
     * The bolt will only emit the field "airport city", "airport code", "call sign"
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("airport city", "airport code", "call sign", "vertical rate"));
    }

    /**
     * This function read airport.txt file and save all information of airports
     *
     * @param filename
     * @throws IOException
     */
    void readAirportData(String filename) throws IOException {
        fileReader = new FileReader(filename);
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        airports = new String[40];
        int i = 0;
        int j = 1;
        while (i != 40) {
            String str = bufferedReader.readLine();
            if (j % 2 != 0) {
                airports[i] = str;
                i++;
            }
            j++;
        }
    }

    /**
     * this method distance latitude and longitude between flight airport
     * ref: https://sciencing.com/convert-degrees-latitude-miles-5744407.html
     *
     * @param value1
     * @param value2
     * @return
     */
    double distanceCalculation(double value1, double value2) {
        if (value1 > value2) {
            return value1 - value2;
        } else {
            return value2 - value1;
        }
    }
}
