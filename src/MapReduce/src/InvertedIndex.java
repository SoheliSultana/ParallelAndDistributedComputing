import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class InvertedIndex {


    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        JobConf conf;

        public void configure(JobConf job) {
            this.conf = job;
        }

        public void map(LongWritable docId, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            // retrieve # keywords from JobConf
            int argc = Integer.parseInt(conf.get("argc"));
            // / get the current file name
            FileSplit fileSplit = (FileSplit) reporter.getInputSplit();
            String filename = "" + fileSplit.getPath().getName();
            FileSystem fs = fileSplit.getPath().getFileSystem(conf);
            String line;
            Hashtable<String, Integer> table = new Hashtable<String, Integer>();
            //tokenize each filesplits by space
            StringTokenizer tokenizer = new StringTokenizer(value.toString(), " ");
            while (tokenizer.hasMoreTokens()) {
                String word = tokenizer.nextToken();
                for (int i = 0; i < argc; i++) {
                    if (word.equals(conf.get("keyword" + i))) {
                        if (table.containsKey(word)) {
                            int count = 1 + table.get(word);
                            table.put(word, count);
                        } else {
                            table.put(word, 1);
                        }
                    }
                }
            }
            //collect output of map from table
            for (String term : table.keySet()) {
                Text posting = new Text(filename + " " + table.get(term));
                output.collect(new Text(term), posting);
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            // actual computation is here.
            Hashtable<String, Integer> table = new Hashtable<String, Integer>();
            //iterates values
            while (values.hasNext()) {
                String posting = values.next().toString();
                String[] splits = posting.split(" ");
                for (int i = 0; i < splits.length; i += 2) {
                    if (table.containsKey(splits[i])) {
                        table.put(splits[i], table.get(splits[i]) + Integer.parseInt(splits[i + 1]));
                    } else {
                        table.put(splits[i], Integer.parseInt(splits[i + 1]));
                    }
                }
            }

            StringBuilder stringBuilder = new StringBuilder();
            boolean first = true;
            for (String docId : table.keySet()) {
                if (first) {
                    stringBuilder.append(docId + " " + table.get(docId));
                    first = false;
                } else {
                    stringBuilder.append(" " + docId + " " + table.get(docId));
                }
            }
            Text docListText = new Text();
            docListText.set(stringBuilder.toString().trim());
            output.collect(key, docListText);
        }
    }

    public static void main(String[] args) throws Exception {

        JobConf conf = new JobConf(InvertedIndex.class);
        conf.setJobName("InversedIndex");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        long startTime = System.currentTimeMillis();
        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path(args[0]));  // input directory name
        FileOutputFormat.setOutputPath(conf, new Path(args[1])); // output directory name
        conf.set("argc", String.valueOf(args.length - 2));// argc maintains #keywords
        for (int i = 0; i < args.length - 2; i++) {
            conf.set("keyword" + i, args[i + 2]);     // keyword1, keyword2, ...
        }
        JobClient.runJob(conf);
        long endTime = System.currentTimeMillis();
        System.out.println(String.format("Time = %d", endTime - startTime));
    }

}
