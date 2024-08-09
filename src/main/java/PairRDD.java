import com.google.common.collect.Iterables;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.sources.In;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class PairRDD {

    public static void main(String[] args) {
        List<String> inputData = new ArrayList<>();
        inputData.add("WARN: Tuesday 4 Sept 0900");
        inputData.add("ERROR: Wednesday 5 Sept 0922");
        inputData.add("FATAL: Thursday 6 Sept 0812");
        inputData.add("ERROR: Friday 7 Sept 0600");
        inputData.add("WARN: Friday 7 Sept 0934");

        SparkConf conf = new SparkConf().setAppName("StartingSpark").setMaster("local[*]");
        try (JavaSparkContext context = new JavaSparkContext(conf)) {
            JavaRDD<String> logMessages = context.parallelize(inputData);
            JavaPairRDD<String, String> pairRDD =  logMessages.mapToPair(input -> {
                String[] columns = input.split(":");
                String loggingLevel = columns[0];
                String dateTime = columns[1];
                return new Tuple2<>(loggingLevel,dateTime);
            });
            pairRDD.collect().forEach(System.out::println);
            JavaPairRDD<String, Iterable<String>> groupedByKey = pairRDD.groupByKey();
            groupedByKey.collect().forEach(System.out::println);
            groupedByKey.collect().forEach(val -> System.out.println(val._1 + " has " + Iterables.size(val._2) + " count"));

            logMessages.mapToPair(input -> new Tuple2<>(input.split(":")[0],1))
                    .reduceByKey((a, b) -> a + b).collect().forEach(System.out::println);
        }

    }

}
