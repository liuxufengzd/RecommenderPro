package org.liu.app.dws;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.liu.app.BaseApp;
import org.liu.constant.Constant;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;

public class DWS_Rating extends BaseApp {
    public static void main(String[] args) {
        new DWS_Rating().run(args, 4);
    }

    @Override
    public void process(String[] args, SparkSession spark) {
        Dataset<Row> rating = TableReader(Constant.LAYER_ODS, "Rating").filter("isnotnull(mid)");
        Dataset<Row> moreMonthRating = rating.select(
                        col("mid"),
                        col("score"),
                        date_format(col("time"), "yyyy-MM").as("yearmonth")
                ).groupBy("mid", "yearmonth")
                .agg(count("score").as("rating_count"))
                .orderBy(col("yearmonth").desc(), col("rating_count").desc());

        moreMonthRating.cache();

        TableWriter(moreMonthRating.withColumn("date", lit(args[0])), Constant.LAYER_DWS, "MovieCountRatingMonth", "date");

        Dataset<Row> moreRating = moreMonthRating.groupBy("mid")
                .agg(sum("rating_count").as("rating_count"))
                .orderBy(col("rating_count").desc());

        TableWriter(moreRating.withColumn("date", lit(args[0])), Constant.LAYER_DWS, "MovieCountRating", "date");

        Dataset<Row> avgRating = rating.groupBy("mid")
                .agg(avg("score").as("avg"))
                .orderBy(col("avg").desc());

        TableWriter(avgRating.withColumn("date", lit(args[0])), Constant.LAYER_DWS, "MovieAvgRating", "date");
    }
}
