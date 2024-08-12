import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.types.DataTypes;

public class UDFExample {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("TestingSparkSQL").master("local[*]").getOrCreate();
        Dataset<Row> dataset = sparkSession.read().option("header", true).csv("src/main/resources/exams/students.csv");
        sparkSession.udf().register("hasPassed",(String grade, String subject) -> {
            if (subject.equals("Biology")) {
                if (grade.startsWith("A") ) {
                    return true;
                }
            } else if (grade.startsWith("A") || grade.equals("B") || grade.equals("C")) {
                 return true;
            }
            return false;
        }, DataTypes.BooleanType);
        Dataset<Row> datasetWithColumn = dataset.withColumn("hasPassed", callUDF("hasPassed",col("grade"), col("subject")));
        datasetWithColumn.show();

        datasetWithColumn = dataset.withColumn("hasPassed", lit(col("grade").equalTo("A+")));
        datasetWithColumn.show();


    }
}
