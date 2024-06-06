package org.liu.app.dim;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.liu.app.BaseApp;
import org.liu.constant.Constant;

import static org.apache.spark.sql.functions.lit;

public class DIM_Movie extends BaseApp {
    public static void main(String[] args) {
        new DIM_Movie().run(args, 4);
    }

    @Override
    public void process(String[] args, SparkSession spark) {
        Dataset<Row> source = DatabaseReader("t_movie");
        source = source
                .filter("isnotnull(mid)")
                .withColumn("date", lit(args[0]));
        TableWriter(source, Constant.LAYER_DIM, "Movie", "date");
    }
}
