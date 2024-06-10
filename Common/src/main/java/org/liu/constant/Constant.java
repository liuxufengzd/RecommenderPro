package org.liu.constant;

public class Constant {
    public static final String PARTITION_OVERWRITE_MODE = "dynamic";
    public static final String WAREHOUSE_BATCH_DIR = "hdfs://hadoop102:8020/recommender/lakehouse/batch";
    public static final String WAREHOUSE_STREAM_DIR = "hdfs://hadoop102:8020/recommender/lakehouse/stream";
    public static final String METASTORE_URI = "thrift://hadoop102:9083";
    public static final String SPARK_EXTENSIONS = "io.delta.sql.DeltaSparkSessionExtension";
    public static final String SPARK_CATALOG = "org.apache.spark.sql.delta.catalog.DeltaCatalog";
    public static final String KAFKA_BOOTSTRAP_SERVERS = "hadoop102:9092,hadoop103:9092";
    public static final String ROCKSDB_STATE_STORE = "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider";
    public static final String DB_USERNAME = "root";
    public static final String DB_PWD = "000000";
    public static final String DB_DRIVER = "com.mysql.cj.jdbc.Driver";
    public static final String DB_HOST = "hadoop102:3306";
    public static final String DB_DATABASE = "recommender";
    public static final String DELTA_DB = "Recommender";
    public static final String LAYER_ODS = "OperationalDataStore";
    public static final String LAYER_DWD = "DataWarehouseDetail";
    public static final String LAYER_DWS = "DataWarehouseSummary";
    public static final String LAYER_ADS = "AppDataStore";
    public static final String LAYER_DIM = "Dimension";
    public static final int FETCH_MOVIE_NUMBER = 20;
    public static final int FETCH_RECENT_RATING_NUMBER = 30;

    // Topics
    public static final String Topic_Tag = "tag";
    public static final String Topic_Rating = "rating";
}
