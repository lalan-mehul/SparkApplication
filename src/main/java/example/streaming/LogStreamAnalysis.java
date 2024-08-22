package example.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;

public class LogStreamAnalysis {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("StartingSpark").setMaster("local[*]");
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(2));

        JavaReceiverInputDStream<String> inputDStream = sc.socketTextStream("localhost", 8989);
        JavaDStream<String> javaDStream = inputDStream.map(item -> item);

        javaDStream = javaDStream.map(input -> input.split(",")[0]);
        JavaPairDStream<String, Long> javaPairDStream = javaDStream.mapToPair(input -> new Tuple2<>(input, 1L));
        //javaPairDStream = javaPairDStream.reduceByKey((x, y) -> x + y);

        javaPairDStream = javaPairDStream.reduceByKeyAndWindow(Long::sum, Durations.minutes(2));
        javaPairDStream.print();

        sc.start();
        sc.awaitTermination();
    }
}
