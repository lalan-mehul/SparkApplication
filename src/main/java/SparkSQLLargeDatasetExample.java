import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSQLLargeDatasetExample {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("TestingSparkSQLWithLargeData").master("local[*]").getOrCreate();
        Dataset<Row> dataset = sparkSession.read().option("header", true).csv("src/main/resources/biglog.txt");

        dataset.createOrReplaceTempView("my_log_table");
        Dataset<Row> results = sparkSession.sql("select level, date_format(dateTime,'MM') as month_number, date_format(dateTime,'MMMM') as month_name, count(level) as totals from my_log_table group by level, month_name, month_number order by month_number, level");
        results = results.drop("month_number");
        results.show(60);

        results.createOrReplaceTempView("aggregated_data");
        Dataset<Row> totalData = sparkSession.sql("select sum(totals) from aggregated_data");
        totalData.show();

        sparkSession.close();
    }

}
