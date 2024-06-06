package org.liu.app.dws;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.liu.app.BaseApp;
import org.liu.constant.Constant;

import static org.apache.spark.sql.functions.*;

public class DWS_MovieRating extends BaseApp {
    public static void main(String[] args) {
        new DWS_MovieRating().run(args, 4);
    }

    @Override
    public void process(String[] args, SparkSession spark) {
        Dataset<Row> rating = TableReader(Constant.LAYER_DWS, "MovieAvgRating")
                .select("mid", "avg");
        Dataset<Row> movie = TableReader(Constant.LAYER_DIM, "Movie")
                .select("mid", "name", "genres");

        movie = movie.join(rating, "mid")
                .select(
                        col("mid"),
                        col("name"),
                        explode(split(col("genres"), "\\|")).as("genre"),
                        col("avg")
                );

        WindowSpec window = Window.partitionBy(trim(col("genre"))).orderBy(desc("avg"));
        movie = movie
                .withColumn("rank", row_number().over(window))
                .filter("rank <= 10")
                .withColumn("date", lit(args[0]));

        TableWriter(movie, Constant.LAYER_DWS, "GenresTopMovies", "date");
    }
}
