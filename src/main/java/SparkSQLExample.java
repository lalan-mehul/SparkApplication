import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSQLExample {

    public static void main(String[] args) {
        try (SparkSession sparkSession = SparkSession.builder().appName("TestingSparkSQL").master("local[*]").getOrCreate()) {
            Dataset<Row> dataset = sparkSession.read().option("header", true).csv("src/main/resources/exams/students.csv");
            dataset.show();
            System.out.println("Row count: " + dataset.count());
            Row firstRow = dataset.first();
            int year = Integer.parseInt(firstRow.getAs("year"));
            System.out.println("Year is:" + year);

            String subject = String.valueOf(firstRow.get(2));
            System.out.println("Subject: " + subject);

            //using expression as filter
            Dataset<Row> modernArtData = dataset.filter("subject='Modern Art' and year >= 2007");
            modernArtData.show();
            System.out.println(modernArtData.count());

            //using filter lambda function
            Dataset<Row> modernArtDataWithFilterFunction = dataset.filter((FilterFunction<Row>) row -> row.getAs("subject").equals("Modern Art"));
            modernArtDataWithFilterFunction.show();

            //using column conditions as filter
            Column subjectColumn = dataset.col("subject");
            Column yearColumn = dataset.col("year");
            Dataset<Row> modernArt = dataset.filter(subjectColumn.equalTo("Modern Art").and(yearColumn.geq(2007)));
            modernArt.show();

            // creating a view to run any SQL query on the data
            dataset.createOrReplaceTempView("my_students_table");
            Dataset<Row> sqlResults = sparkSession.sql("select avg(score), year from my_students_table where year >= 2007 and subject = 'Modern Art' group by year order by year desc");
            sqlResults.show();


        }

    }
}
