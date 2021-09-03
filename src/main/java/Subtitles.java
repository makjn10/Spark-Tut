import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

import static org.apache.log4j.Level.WARN;

public class Subtitles {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
        Logger.getLogger("org.apache").setLevel(WARN);

        SparkConf conf = new SparkConf()
                .setAppName("Subtitles Processor")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> inputRDD = sc.textFile("src/main/resources/input-spring.txt");
        inputRDD.map(line -> line.replaceAll("[^a-zA-Z\\s]", "").toLowerCase())
                .flatMap(v -> Arrays.asList(v.split(" ")).iterator())
                .filter(word -> word.length() > 0)
                .filter(Util::isNotBoring)
                .mapToPair(word -> new Tuple2<>(word, 1L))
                .reduceByKey(Long::sum)
                .mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1))
                .sortByKey(false)
                .take(10)
                .forEach(tuple -> System.out.println(tuple._2 + " has " + tuple._1 + " occurances."));
    }
}
