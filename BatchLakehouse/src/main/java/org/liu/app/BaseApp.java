package org.liu.app;

import io.delta.tables.DeltaTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.liu.constant.Constant;
import org.liu.util.Utils;

import java.io.Serializable;
import java.util.Properties;

public abstract class BaseApp implements Serializable {
    private SparkSession spark;

    public abstract void process(String[] args, SparkSession spark);

    public void run(String[] args, int parallelism) {
        spark = SparkSession.builder()
                .appName(this.getClass().getSimpleName())
                .master("yarn")
                .config("spark.sql.shuffle.partitions", parallelism)
                .config("spark.sql.sources.partitionOverwriteMode", Constant.PARTITION_OVERWRITE_MODE)
                .config("spark.sql.warehouse.dir", Constant.WAREHOUSE_DIR)
                .config("hive.metastore.uris", Constant.METASTORE_URI)
                .config("spark.sql.adaptive.enabled", true)
                .config("spark.sql.extensions", Constant.SPARK_EXTENSIONS)
                .config("spark.sql.catalog.spark_catalog", Constant.SPARK_CATALOG)
                .enableHiveSupport()
                .getOrCreate();
        process(args, spark);
        spark.stop();
    }

    public Dataset<Row> KafkaReader(String topic) {
        return spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", Constant.KAFKA_BOOTSTRAP_SERVERS)
                .option("subscribe", topic)
                .load()
                .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
    }

    public Dataset<Row> DatabaseReader(String tableName) {
        String url = "jdbc:mysql://" + Constant.DB_HOST + "/" + Constant.DB_DATABASE + "?useSSL=false&allowPublicKeyRetrieval=true&useUnicode=true&characterEncoding=utf-8";
        Properties properties = new Properties();
        properties.put("driver", Constant.DB_DRIVER);
        properties.put("user", Constant.DB_USERNAME);
        properties.put("password", Constant.DB_PWD);

        return spark.read().jdbc(url, tableName, properties);
    }

    public Dataset<Row> TableReader(String layer, String tableName) {
        return DeltaTable.forName(spark, Utils.getTableName(layer, tableName)).toDF();
    }

    public void TableWriter(Dataset<Row> dataset, String layer, String tableName, String[] partitionColumns, boolean isStreamSource) {
        if (isStreamSource) {
            try {
                dataset.writeStream()
                        .format("delta")
                        .partitionBy(partitionColumns)
                        .option("checkpointLocation", Utils.getTableCheckpointPath(layer, tableName))
                        .option("path", Utils.getTablePath(layer, tableName))
                        .toTable(Utils.getTableName(layer, tableName))
                        .awaitTermination();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            if (dataset.isEmpty()) return;
            dataset.write()
                    .format("delta")
                    .mode(SaveMode.Append)
                    .partitionBy(partitionColumns)
                    .option("path", Utils.getTablePath(layer, tableName))
                    .saveAsTable(Utils.getTableName(layer, tableName));
        }
    }

    public void TableWriter(Dataset<Row> dataset, String layer, String tableName, String partitionColumn, boolean isStreamSource) {
        TableWriter(dataset, layer, tableName, new String[]{partitionColumn}, isStreamSource);
    }

    public void TableWriter(Dataset<Row> dataset, String layer, String tableName, String[] partitionColumns) {
        TableWriter(dataset, layer, tableName, partitionColumns, false);
    }

    public void TableWriter(Dataset<Row> dataset, String layer, String tableName, String partitionColumn) {
        TableWriter(dataset, layer, tableName, new String[]{partitionColumn}, false);
    }
}