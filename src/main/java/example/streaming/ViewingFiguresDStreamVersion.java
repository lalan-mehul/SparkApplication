package example.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class ViewingFiguresDStreamVersion {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("ViewingFigures").setMaster("local[*]");
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(1));

    }
}
