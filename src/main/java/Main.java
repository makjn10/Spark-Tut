import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

import static org.apache.log4j.Level.WARN;

public class Main {
    public static void main(String[] args) {
        List<Double> input = new ArrayList<>();
        input.add(1.2);
        input.add(1.5);
        input.add(2.6);
        input.add(1.8);
        input.add(5.0);

        //Suppress unnecessary logs
        //Logger.getLogger("org.apache").setLevel(WARN);

        //represents configuration of spark
        //local[*] means use all the available cores in local machine (NOTE : not fully accurate)
        SparkConf conf = new SparkConf()
                .setAppName("StartingSpark")
                .setMaster("local[*]");

        //connection to a spark cluster or consider it an entrypoint to spark
        JavaSparkContext sc = new JavaSparkContext(conf);

        //Load data in RDD
        /*
            Spark is written in Scala.
            JavaRDD is communicating with a SCALA RDD under the hood.
            We can call regular Java methods.
        */
        JavaRDD<Double> inputRDD = sc.parallelize(input);
        System.out.println(inputRDD);
        sc.close();
    }
}
