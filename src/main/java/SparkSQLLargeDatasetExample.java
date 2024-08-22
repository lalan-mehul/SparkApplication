import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;

public class SparkSQLLargeDatasetExample {

    public static void main(String[] args) {
        long startTime = System.nanoTime();
        SparkSession sparkSession = SparkSession.builder().appName("TestingSparkSQLWithLargeData").master("local[*]").getOrCreate();
        Dataset<Row> dataset = sparkSession.read().option("header", true).csv("src/main/resources/biglog.txt");

        SimpleDateFormat input = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        SimpleDateFormat monthNumber = new SimpleDateFormat("MM");
        SimpleDateFormat monthName = new SimpleDateFormat("MMMM");

        sparkSession.udf().register("month_number",(String dateTime) -> {
            Date date = input.parse(dateTime);
            return monthNumber.format(date);
        }, DataTypes.StringType);

        sparkSession.udf().register("month_name", (String dateTime) -> {
            Date date = input.parse(dateTime);
            return monthName.format(date);
        }, DataTypes.StringType);

        dataset.createOrReplaceTempView("my_log_table");
        Dataset<Row> results = sparkSession.sql("select level, month_name(dateTime) as month_name, month_number(dateTime) as month_number, count(level) as totals from my_log_table group by level, month_name, month_number order by month_number, level");
        results = results.drop("month_number");
        results.show(60);
        results.explain();

        results.createOrReplaceTempView("aggregated_data");
        Dataset<Row> totalData = sparkSession.sql("select sum(totals) from aggregated_data");
        totalData.show();

        //Scanner scanner = new Scanner(System.in);
        //scanner.nextLine();

        sparkSession.close();
        System.out.println( "Time taken: " +  (System.nanoTime() -startTime)/1000000 ) ;

    }

}
