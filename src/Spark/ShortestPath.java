import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class ShortestPath {
    static int shortestDistance = Integer.MAX_VALUE;

    public static void main(String[] args) {
        // start Sparks and read a given input file
        System.out.println("Starting shortest path");
        String inputFile = args[0];
        final Pattern SPACES = Pattern.compile("=|,|;");
        SparkConf conf = new SparkConf().setAppName("BFS-based Shortest Path Search");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        // read input file into RDD lines
        JavaRDD<String> lines = jsc.textFile(inputFile);
        // now start a timer
        long startTime = System.currentTimeMillis();
        // creats a key, value pair, where key is each vertex and value
        // is information of list of <neighbor, weight>, distance, prev(distance)and status
        JavaPairRDD<String, Data> network = lines.mapToPair(s -> {
            String[] parts = SPACES.split(s);
            List<Tuple2<String, Integer>> neighbors = new ArrayList<>();
            for (int i = 1; i < parts.length; i += 2) {
                neighbors.add(new Tuple2(parts[i], Integer.parseInt(parts[i + 1])));
                //System.out.println("parts"+parts[i]+" "+parts[i+1]);
            }
            Data data = new Data();
            data.neighbors = neighbors;
            data.prev = Integer.MAX_VALUE;
            data.distance = Integer.MAX_VALUE;
            if (parts[0].equals(args[1])) {
                data.status = "ACTIVE";
                data.distance = 0;
            }
            return new Tuple2<>(parts[0], data);
        });
        // continue the process until there is any active vertex
        while (true) {
            // For active vertex, creating tuple for each neighbor<neighbor, Data>,
            // upadating distance with a new distance, adding each Tuple to a list and
            // returing the list
            JavaPairRDD<String, Data> propagatedNetwork = network.flatMapToPair(vertex -> {
                List<Tuple2<String, Data>> newNeighbors = new ArrayList<>();

                if (vertex._2().status.equals("ACTIVE")) {
                    for (Tuple2 neighbor : vertex._2().neighbors) {
                        Data data = new Data();
                        data.distance = vertex._2().distance + neighbor._2$mcI$sp();
                        newNeighbors.add(new Tuple2(neighbor._1(), data));
                    }

                    vertex._2().status = "INACTIVE";

                }

                newNeighbors.add(vertex);
                return newNeighbors.iterator();
            });

            // taking action for each key: find the shortest distance for each vertex(key)
            // update the disatnce attributes with the new shortest distance
            network = propagatedNetwork.reduceByKey((K1, K2) -> {
                int min = Math.min(K1.distance, K2.distance);

                if (K1.neighbors.size() > 0) {
                    K1.distance = min;
                    return K1;
                } else if (K2.neighbors.size() > 0) {
                    K2.distance = min;
                    return K2;
                } else {
                    K1.distance = min;
                    return K1;
                }
            });

            // if a new distance is shorter than prev diatnce then update the prev
            // distance with shortest distance and active the status for that vertex
            network = network.mapValues(value -> {
                if (value.prev > value.distance) {
                    value.prev = value.distance;
                    value.status = "ACTIVE";
                }
                return value;
            });

            long activeCount = network.filter(vertex -> vertex._2().status.equals("ACTIVE")).count();

            if (activeCount == 0) break;


        }
        // finishing the time
        long endTime = System.currentTimeMillis();
        // retrieve the network to get distance of destination vertex
        Map<String, Data> maps = network.collectAsMap();
        shortestDistance = maps.get(args[2]).distance;

        System.out.println("Time required = " + (endTime - startTime));
        System.out.println(" From vertex " + args[1] + " to vertex " + args[2] + " takes distance = " + shortestDistance);

        jsc.stop();
    }
}
