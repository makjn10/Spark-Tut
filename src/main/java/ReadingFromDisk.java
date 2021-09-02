import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.log4j.Level.WARN;

public class ReadingFromDisk {
    public static void main(String[] args) {
        // If running in windows copy hadoop folder in resources to C:/ drive
        System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
        Logger.getLogger("org.apache").setLevel(WARN);

        SparkConf conf = new SparkConf()
                .setAppName("StartingSpark")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        sc.textFile("src\\main\\resources\\biglog.txt")
                .mapToPair(line -> new Tuple2<>(line.split(",")[0], 1))
                .filter(tuple -> !(tuple._1.equals("level")))
                .reduceByKey(Integer::sum)
                .collect()
                .forEach(System.out::println);
    }
}

