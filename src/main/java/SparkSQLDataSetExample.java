import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import java.util.ArrayList;
import java.util.List;

public class SparkSQLDataSetExample {
    public static void main(String[] args) {
        List<Row> inMemory = new ArrayList<>();
        inMemory.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
        inMemory.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
        inMemory.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
        inMemory.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
        inMemory.add(RowFactory.create("FATAL","2015-4-21 19:23:20"));

        StructField[] fields = new StructField[] {
                new StructField("level", DataTypes.StringType,false, Metadata.empty()),
                new StructField("dateTime", DataTypes.StringType,false, Metadata.empty()),
        };
        StructType schema = new StructType(fields);
        SparkSession sparkSession = SparkSession.builder().appName("TestingSparkSQL").master("local[*]").getOrCreate();
        Dataset<Row> inMemoryData = sparkSession.createDataFrame(inMemory, schema);
        inMemoryData.show();

        inMemoryData.createOrReplaceTempView("my_log_table");
        Dataset<Row> groupedData = sparkSession.sql("select count(*), level from my_log_table group by level");
        groupedData.show();

        Dataset<Row> results = sparkSession.sql("select count(level) as total, level, date_format(dateTime,'MMMM') as month_name from my_log_table group by level, month_name");
        results.show();

    }
}
