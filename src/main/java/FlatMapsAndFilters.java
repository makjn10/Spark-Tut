import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.log4j.Level.WARN;

public class FlatMapsAndFilters {
    public static void main(String[] args) {
        List<String> logData = new ArrayList<>();
        logData.add("DEBUG 2015-2-6 16:24:07 u");
        logData.add("WARN 2016-7-26 18:54:43 k");
        logData.add("INFO 2012-10-18 14:35:19 l");
        logData.add("DEBUG 2012-4-26 14:26:50 o");
        logData.add("DEBUG 2013-9-28 20:27:13 p");

        Logger.getLogger("org.apache").setLevel(WARN);

        SparkConf conf = new SparkConf()
                .setAppName("StartingSpark")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        sc.parallelize(logData)
                .flatMap(value -> Arrays.asList(value.split(" ")).iterator())
                .filter(word -> word.length() != 1)
                .collect()
                .forEach(System.out::println);
    }
}
