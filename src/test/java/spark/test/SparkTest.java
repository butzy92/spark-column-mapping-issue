package spark.test;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SparkTest {

    private SparkSession sparkSession;


    @AfterEach
    void after() {
        sparkSession.close();
    }

    @Test
    void withPruningEnabledAndMapSingleColumn() {
        sparkSession = getSparkWithPrune("true");

        Dataset<Row> csv = sparkSession.read()
                .schema("first INT, corrupt STRING")
                .option("columnNameOfCorruptRecord", "corrupt")
                .csv("test3Columns.csv");

        csv.show();
        csv.createOrReplaceTempView("tempView");
        Dataset<Row> fromSQL = csv.sqlContext()
                .sql("select first from tempView where corrupt is null");
        fromSQL.show();
        assertEquals(fromSQL.collectAsList().size(), 2);
    }

    @Test
    void withPruningEnabledAndMap2ColumnsButUse1InSql() {
        sparkSession = getSparkWithPrune("true");

        Dataset<Row> csv = sparkSession.read()
                .schema("first INT,notUsed STRING, corrupt STRING")
                .option("columnNameOfCorruptRecord", "corrupt")
                .csv("test3Columns.csv");

        csv.show();
        csv.createOrReplaceTempView("tempView");
        Dataset<Row> fromSQL = csv.sqlContext()
                .sql("select first from tempView where corrupt is null");
        fromSQL.show();
        assertEquals(fromSQL.collectAsList().size(), 2);
    }

    @Test
    void withPruningDisableAndMap2ColumnsButUse1InSql() {
        sparkSession = getSparkWithPrune("false");

        Dataset<Row> csv = sparkSession.read()
                .schema("first INT,notUsed STRING, corrupt STRING")
                .option("columnNameOfCorruptRecord", "corrupt")
                .csv("test3Columns.csv");

        csv.show();
        csv.createOrReplaceTempView("tempView");
        Dataset<Row> fromSQL = csv.sqlContext()
                .sql("select first from tempView where corrupt is null");
        fromSQL.show();
        assertEquals(fromSQL.collectAsList().size(), 0);
    }

    private SparkSession getSparkWithPrune(String s) {
        return SparkSession.builder()
                .master("local")
                .appName("pcaas_ng_transformer")
                .config("spark.locality", "PROCESS_LOCAL")
                .config("spark.sql.csv.parser.columnPruning.enabled", s)
                .config("spark.ui.enabled", "false")
                .config("spark.eventLog.enabled", "false")
                .config("spark.sql.inMemoryColumnarStorage.compressed", "false")
                .config("spark.sql.shuffle.partitions", 1)
                .getOrCreate();
    }


}
