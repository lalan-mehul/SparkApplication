import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        List<Double> inputData = new ArrayList<>();
        inputData.add(10.2);
        inputData.add(9.3);
        inputData.add(7.2);
        inputData.add(65.33);

        SparkConf conf = new SparkConf().setAppName("StartingSpark").setMaster("local[*]");
        try (JavaSparkContext context = new JavaSparkContext(conf)) {
            JavaRDD<Double> rdd = context.parallelize(inputData);
            Double returnValue = rdd.reduce(  (a,b) -> a + b );
            System.out.println("Sum of elements in RDD: " + returnValue);

            JavaRDD<Pair<Double, Double>> doubledRDD =  rdd.map((a) -> new Pair(a, a*2));
            doubledRDD.collect().forEach(System.out::println);

            // get count of elements
            System.out.println("RDD element count using count method on RDD: " + doubledRDD.count());

            // count using map/reduce
            Integer countByMapReduce = doubledRDD.map(a -> 1).reduce((a,b) -> a+b);
            System.out.println("RDD element count using map/reduce:" + countByMapReduce);
        }

    }
}
