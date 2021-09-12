import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Arrays;
import java.util.Iterator;

class FlatMapFunctionS implements FlatMapFunction<String, String> {

    public Iterator<String> call(String s) {
        return Arrays.asList(s.split(" ")).iterator();
    }

}



