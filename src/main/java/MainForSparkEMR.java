import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import util.Util;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class MainForSparkEMR {

    public static class WordCountComparator implements Comparator<Tuple2<String, Long>>, Serializable {
        @Override
        public int compare(Tuple2<String, Long> tuple1, Tuple2<String, Long> tuple2) {
            return tuple2._2.compareTo(tuple1._2);
        }
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("StartingSpark");
        try (JavaSparkContext context = new JavaSparkContext(conf)) {
            JavaRDD<String> subtitleLines = context.textFile("s3a://mehul-spark-files/input.txt");
            JavaRDD<String> lettersOnlyRdd = subtitleLines.map(text -> text.replaceAll("[^a-zA-Z\\s]", "").toLowerCase());
            JavaRDD<String>  blanksRemovedRdd = lettersOnlyRdd.filter(sentence -> !sentence.trim().isEmpty());

            JavaRDD<String> subtitleWords = blanksRemovedRdd.flatMap(input -> Arrays.asList(input.split(" ")).iterator());
            JavaRDD<String> filteredWords = subtitleWords.filter(word -> Util.isNotBoring(word) && !word.trim().isEmpty());

            JavaPairRDD<String, Long> wordsWithCount = filteredWords.mapToPair(word -> new Tuple2<>(word,1L));
            JavaPairRDD<String, Long> wordsSummed = wordsWithCount.reduceByKey(Long::sum);

            List<Tuple2<String, Long>> topOrderedWords = wordsSummed.takeOrdered(10, new WordCountComparator());
            System.out.println(topOrderedWords);

        }

    }
}