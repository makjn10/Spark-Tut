import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.sql.sources.In;
import scala.Int;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Joins {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("SortAndCoalesce");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<Integer, Integer>> visits = new ArrayList<>();
        visits.add(new Tuple2<>(2, 7));
        visits.add(new Tuple2<>(4, 9));
        visits.add(new Tuple2<>(5, 8));
        visits.add(new Tuple2<>(6, 1));

        List<Tuple2<Integer, String>> names = new ArrayList<>();
        names.add(new Tuple2<>(1, "Naman"));
        names.add(new Tuple2<>(4, "Mayank"));
        names.add(new Tuple2<>(5, "Archit"));

        JavaPairRDD<Integer, Integer> visitRdd = sc.parallelizePairs(visits);
        JavaPairRDD<Integer, String> namesRdd = sc.parallelizePairs(names);

        JavaPairRDD<Integer, Tuple2<Integer, String>> innerJoinedRDD = visitRdd.join(namesRdd);
        JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> leftJoined = visitRdd.leftOuterJoin(namesRdd);
        JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> rightJoined = visitRdd.rightOuterJoin(namesRdd);
        JavaPairRDD<Integer, Tuple2<Optional<Integer>, Optional<String>>> fullOuterJoined = visitRdd.fullOuterJoin(namesRdd);
        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, String>> cartesianJoined = visitRdd.cartesian(namesRdd);

        innerJoinedRDD.collect().forEach(System.out::println);
        System.out.println("----");
        leftJoined.collect().forEach(e -> System.out.println(e._2._1 + " visits -> " + e._2._2.orElse("NO NAME PRESENT")));
        System.out.println("----");
        rightJoined.collect().forEach(e -> System.out.println(e._2._1.orElse(0) + " visits -> " + e._2._2));
        System.out.println("----");
        fullOuterJoined.collect().forEach(e -> System.out.println(e._2._1.orElse(0) + " visits -> " + e._2._2.orElse("NO NAME PRESENT")));
        System.out.println("----");
        cartesianJoined.collect().forEach(System.out::println);
    }
}
