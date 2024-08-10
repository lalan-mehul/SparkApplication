import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class JoinsExample {

    public static void main(String[] args) {
        List<Tuple2<Integer, Integer>> visits = new ArrayList<>();
        visits.add(new Tuple2<>(1, 37));
        visits.add(new Tuple2<>(2, 12));
        visits.add(new Tuple2<>(3, 15));
        visits.add(new Tuple2<>(8, 20));

        List<Tuple2<Integer, String>> users = new ArrayList<>();
        users.add(new Tuple2<>(1, "Raj"));
        users.add(new Tuple2<>(2, "Nayan"));
        users.add(new Tuple2<>(3, "Shrusti"));
        users.add(new Tuple2<>(4, "Akash"));

        SparkConf conf = new SparkConf().setAppName("StartingSpark").setMaster("local[*]");
        try (JavaSparkContext context = new JavaSparkContext(conf)) {
            JavaPairRDD<Integer, Integer> visitsRDD = context.parallelizePairs(visits);
            JavaPairRDD<Integer, String> usersRDD = context.parallelizePairs(users);

            JavaPairRDD<Integer, Tuple2<Integer, String>> userVisits = visitsRDD.join(usersRDD);
            userVisits.collect().forEach(System.out::println);
        }
    }
}
