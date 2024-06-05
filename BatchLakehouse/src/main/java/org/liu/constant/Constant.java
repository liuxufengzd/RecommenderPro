package org.liu.constant;

public class Constant {
    public static final String PARTITION_OVERWRITE_MODE = "dynamic";
    public static final String WAREHOUSE_DIR = "hdfs://hadoop102:8020/recommender/lakehouse/batch";
    public static final String METASTORE_URI = "thrift://hadoop102:9083";
    public static final String SPARK_EXTENSIONS = "io.delta.sql.DeltaSparkSessionExtension";
    public static final String SPARK_CATALOG = "org.apache.spark.sql.delta.catalog.DeltaCatalog";
    public static final String KAFKA_BOOTSTRAP_SERVERS = "hadoop102:9092,hadoop103:9092";
    public static final String DB_USERNAME = "root";
    public static final String DB_PWD = "000000";
    public static final String DB_DRIVER = "com.mysql.cj.jdbc.Driver";
    public static final String DB_HOST = "hadoop102:3306";
    public static final String DB_DATABASE = "recommender";
    public static final String DELTA_DB = "Recommender";
    public static final String LAYER_ODS = "OperationalDataStore";
    public static final String LAYER_DWD = "DataWarehouseDetail";
    public static final String LAYER_DWS = "DataWarehouseSummary";

    // Topics
    public static final String Topic_Tag = "tag";
    public static final String Topic_Rating = "rating";
}
