import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;


public class SortsAndCoalesce {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("SortAndCoalesce");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // foreach method doesn't work on sort as data is on multiple partitions
        // foreach method is sent to each partition and is run in parallel in partitions
        //
        /* coalesce(numPartitions)	Decrease the number of partitions in the RDD to numPartitions.
           Useful for running operations more efficiently after filtering down a large dataset to a small amount of data.
           It is pointless to run on multiple partitions if we have reached to a small data (shuffles will be costly on performance)
           coalesce is maily used for performance reasons
         */

        JavaRDD<String> inputRDD = sc.textFile("Tutorial/src/main/resources/input-spring.txt");
        System.out.println(inputRDD.getNumPartitions() + "partitions");
        inputRDD.map(line -> line.replaceAll("[^a-zA-Z\\s]", "").toLowerCase())
                .flatMap(v -> Arrays.asList(v.split(" ")).iterator())
                .filter(word -> word.length() > 0)
                .filter(Util::isNotBoring)
                .mapToPair(word -> new Tuple2<>(word, 1L))
                .reduceByKey(Long::sum)
                .mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1))
                .sortByKey(false)
                .foreach(tuple -> System.out.println(tuple._2 + " has " + tuple._1 + " occurances."));

        /*
        Output is like this :
                knowledgeable has 1 occurances.
                spring has 1100 occurances.
                companyname has 1 occurances.
                dao has 269 occurances.
                nicest has 1 occurances.
                hibernate has 236 occurances.
                popped has 1 occurances.
                aop has 142 occurances.
                grouped has 1 occurances.
                .
                .
                .

        This is because in system the partition threads are running parallel
         */
    }
}
