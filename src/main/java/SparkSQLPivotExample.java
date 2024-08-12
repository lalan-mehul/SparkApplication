import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class SparkSQLPivotExample {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("TestingSparkSQLJavaAPIWithLargeData").master("local[*]").getOrCreate();
        Dataset<Row> dataset = sparkSession.read().option("header", true).csv("src/main/resources/biglog.txt");
        Object[] monthsArr = new Object[] {"January","February","March","April","May","June","July","August","September","October","November","December"};
        List<Object> months = Arrays.asList(monthsArr);

        Dataset<Row> columnSelect = dataset.select(col("level"), date_format(col("dateTime"), "MMMM").alias("month_name"));
        Dataset<Row> pivotData = columnSelect.groupBy(col("level")).pivot(col("month_name"), months).count();
        pivotData.show(100);
    }
}
