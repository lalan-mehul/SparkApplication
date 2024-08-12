import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class PivotTableMultipleAggregations {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("TestingSparkSQL").master("local[*]").getOrCreate();
        Dataset<Row> dataset = sparkSession.read().option("header", true).csv("src/main/resources/exams/students.csv");


        dataset = dataset.groupBy("subject").pivot("year")
                         .agg(round(avg("score"),2).alias("Average"),
                              round(stddev("score"),2).alias("Std-dev"));
        dataset.show();
    }
}
