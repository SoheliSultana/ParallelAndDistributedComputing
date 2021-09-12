import java.util.Arrays;                           // Arrays, List, and Iterator returned from actions
import java.util.Comparator;
import java.io.*;

class MyClassMax implements Serializable, Comparator<Integer> {
    @Override
    public int compare(Integer o1, Integer o2) {
        return Integer.compare(o1, o2);
    }
}
