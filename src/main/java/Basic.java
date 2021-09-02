import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

import static org.apache.log4j.Level.WARN;

public class Basic {
    public static void main(String[] args) {
        List<Double> input = new ArrayList<>();
        input.add(1.2);
        input.add(1.5);
        input.add(2.6);
        input.add(1.8);
        input.add(5.0);

        //Suppress unnecessary logs
        Logger.getLogger("org.apache").setLevel(WARN);

        //represents configuration of spark
        //local[*] means use all the available cores in local machine (NOTE : not fully accurate)
        SparkConf conf = new SparkConf()
                .setAppName("StartingSpark")
                .setMaster("local[*]");

        //connection to a spark cluster or consider it an entrypoint to spark
        JavaSparkContext sc = new JavaSparkContext(conf);

        //Load data in RDD
        /*
            - Spark is written in Scala.
            - JavaRDD is communicating with a SCALA RDD under the hood.
            - We can call regular Java methods.
            - RDD is immutable, therefore we can only create a new RDD applying transformations
        */
        JavaRDD<Double> inputRDD = sc.parallelize(input);

        /*
            - Reduce on RDDs
            - rdd.reduce(function) -> function that takes 2 input parameters of certain type
              and return type is same as input type
        */
        Double total = inputRDD.reduce(Double::sum);
        System.out.println("Total : " + total);

        /*
            - Map on RDDs
            - Map operation on RDD allows us to transform the structure of rdd from one
              form to another
        */
        JavaRDD<Double> sqrtRDD = inputRDD.map(Math::sqrt);
        Double sqrtTotal = sqrtRDD.reduce(Double::sum);
        System.out.println("sqrtTotal : " + sqrtTotal);

        /*
            COUNT NUMBER OF ELEMENTS IN RDD using map & reduce
         */
        JavaRDD<Integer> numRDD = sqrtRDD.map(x -> 1);
        int count = numRDD.reduce(Integer::sum);
        System.out.println("Number of elements : " + count);

        /*
            - foreach() on RDD
            - takes a function as input that has void return type
            - this function should be serializable as Spark sends that function to various partitions
                by serializing the function
            - thus, it does not builds another RDD
        */
        // sqrtRDD.foreach(System.out::println); -> gives Non-Serializable exception as System.out.println()
        // is not serializable
        sqrtRDD.collect().forEach(System.out::println);
        // -collect here collects all the elements in a Java collection, and we are running forEach method
        // of that java collection
        // -but this can increase the memory usage in large data use cases

        /*
            TUPLES
            - Scala Tuples : (2, 3) ; ('hello', 'bye', 'nice')
            - in Java we have a class Tuple2 (in scala library)
            - we have classes like Tuple2, Tuple3, ..... Tuple22
         */
        Tuple2<Double, Integer> myTuple = new Tuple2<>(2.2, 5);
        System.out.println(myTuple._1 + " - " + myTuple._2);

        sc.close();
    }
}
