import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class SparkSQLJavaAPIExample {
    public static void main(String[] args) {
        long startTime = System.nanoTime();

        SparkSession sparkSession = SparkSession.builder().appName("TestingSparkSQLJavaAPIWithLargeData").master("local[*]").getOrCreate();
        Dataset<Row> dataset = sparkSession.read().option("header", true).csv("src/main/resources/biglog.txt");

        Dataset<Row> expressionSelect = dataset.selectExpr("level", "date_format(dateTime,'MMMM') as month_name");
        expressionSelect.show();

        Dataset<Row> columnSelect = dataset.select(col("level"), date_format(col("dateTime"),"MM").alias("month_number"), date_format(col("dateTime"), "MMMM").alias("month_name"));
        RelationalGroupedDataset groupedDataset = columnSelect.groupBy(col("month_name"), col("level"), col("month_number"));

        Dataset<Row> groupedResults = groupedDataset.count();
        //date_format(dateTime,'MM')
        groupedResults = groupedResults.orderBy(col("month_number"), col("level"));
        groupedResults = groupedResults.drop(col("month_number"));
        groupedResults.show(100);
        groupedResults.explain();
        System.out.println( "Time taken: " +  (System.nanoTime() -startTime)/1000000 ) ;
    }
}
