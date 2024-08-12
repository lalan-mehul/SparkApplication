import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class ExamResults {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("TestingSparkSQL").master("local[*]").getOrCreate();
        Dataset<Row> dataset = sparkSession.read().option("header", true).csv("src/main/resources/exams/students.csv");

        Dataset<Row> curtailedDataSet = dataset.select(col("subject"), col("score").cast(DataTypes.IntegerType));
        curtailedDataSet = curtailedDataSet.groupBy("subject").max("score");
        curtailedDataSet.show();


        Dataset<Row> aggregatedData = dataset.groupBy("subject").agg(max(col("score").cast(DataTypes.IntegerType)).alias("max_score"),
                                                                          min(col("score").cast(DataTypes.IntegerType)).alias("min_score"));
        aggregatedData.show();


    }

}
