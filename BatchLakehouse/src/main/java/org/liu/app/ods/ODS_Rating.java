package org.liu.app.ods;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.liu.app.BaseApp;
import org.liu.constant.Constant;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.*;

public class ODS_Rating extends BaseApp {
    public static void main(String[] args) {
        new ODS_Rating().run(args, 4);
    }

    @Override
    public void process(String[] args, SparkSession spark) {
        Dataset<Row> source = KafkaReader(Constant.Topic_Rating);
        source = source.select(
                        from_json(col("value"), getSchema()).as("value")
                ).select(
                        col("value.uid").as("uid"),
                        col("value.mid").as("mid"),
                        col("value.score").as("score"),
                        col("value.time").as("time")
                )
                .withColumn("date", lit(args[0]));
        TableWriter(source, Constant.LAYER_ODS, "Rating", "date", true);
    }

    private StructType getSchema() {
        return new StructType()
                .add("uid", IntegerType)
                .add("mid", IntegerType)
                .add("score", DoubleType)
                .add("time", TimestampType);
    }
}
