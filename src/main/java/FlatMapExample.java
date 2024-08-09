import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FlatMapExample {

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
            // use flapMap and print all the words
            JavaRDD<String> words =  logMessages.flatMap(value -> Arrays.asList(value.split(" ")).iterator());
            words.collect().forEach(System.out::println);

            // filter and remove words with single character from the RDD
            JavaRDD filteredWords = words.filter(word -> word.length() > 1);
            filteredWords.collect().forEach(System.out::println);
        }

    }
}
