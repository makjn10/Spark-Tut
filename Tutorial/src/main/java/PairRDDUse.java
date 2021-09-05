import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

import static org.apache.log4j.Level.WARN;

public class PairRDDUse {
    public static void main(String[] args) {
        /*
            PAIR RDDs
            - We get extra functions on PairRDD
            - Very similar to Java Map but PairRDD can have duplicate keys
         */
        List<String> logData = new ArrayList<>();
        logData.add("DEBUG,2015-2-6 16:24:07");
        logData.add("WARN,2016-7-26 18:54:43");
        logData.add("INFO,2012-10-18 14:35:19");
        logData.add("DEBUG,2012-4-26 14:26:50");
        logData.add("DEBUG,2013-9-28 20:27:13");

        Logger.getLogger("org.apache").setLevel(WARN);
        SparkConf conf = new SparkConf()
                .setAppName("StartingSpark")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> originalLogData = sc.parallelize(logData);
        JavaPairRDD<String, String> logs = originalLogData.mapToPair(rawValue -> {
            String [] separatedVals = rawValue.split(",");
            String logLevel = separatedVals[0];
            String dataAndTime = separatedVals[1];
            return new Tuple2<>(logLevel, dataAndTime);
        });

        /*
            groupByKey function on pairRDD
            - NOTE : this can cause severe performance issues and even crashes in clusters
                     so use it only when no better alternative is not present
            - It wll return a PairRDD with one entry for each key : <K, Iterable<V>>
            - We can not call size() function on Iterable
         */
        JavaPairRDD<String, Iterable<String>> groupedLogs = logs.groupByKey();
        groupedLogs.collect().forEach(System.out::println);

        /*
            reduceByKey function on pairRDD
            - It wll return a PairRDD with one entry for each key : <K, Integer>
         */
        // FLUENT API
        sc.parallelize(logData)
                .mapToPair(rawValue -> new Tuple2<>(rawValue.split(",")[0], 1))
                .reduceByKey(Integer::sum)
                .collect().
                forEach(System.out::println);

        sc.close();
    }
}
