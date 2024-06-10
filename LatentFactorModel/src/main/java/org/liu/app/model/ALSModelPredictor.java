package org.liu.app.model;

import com.google.gson.Gson;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.liu.app.BaseApp;
import org.liu.app.bean.RecommendationLog;
import org.liu.client.HBaseClient;
import org.liu.util.HBaseConnectionUtil;
import scala.collection.JavaConverters;
import scala.collection.mutable.Seq;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.apache.spark.ml.functions.array_to_vector;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.*;
import static org.liu.constant.Constant.*;

public class ALSModelPredictor extends BaseApp {
    public static void main(String[] args) {
        new ALSModelPredictor().run(args, 4);
    }

    @Override
    public void process(String[] args, SparkSession spark) {
        String date = args[0];
        // Offline recommendation is based on all historical records of any user
        Dataset<Row> rating = TableReader(LAYER_ODS, "Rating")
                .select("uid", "mid", "score");

        // Train model using the best parameter set
        ALS als = new ALS().setMaxIter(10).setRank(200).setRegParam(0.1).setUserCol("uid").setItemCol("mid").setRatingCol("score");
        ALSModel model = als.fit(rating);
        model.userFactors().persist();
        model.itemFactors().persist();

        // Predict
        Dataset<Row> users = rating.select("uid").distinct();
        Dataset<Row> movies = rating.select("mid").distinct();
        Dataset<Row> predictions = model.transform(users.crossJoin(movies));
        TableWriter(predictions.withColumn("date", lit(date)), LAYER_DWD, "RatingPredicted", "date");

        // Get top 10 movie recommendations for all users
        Dataset<Row> userRecs = model.recommendForAllUsers(10);
        TableWriter(userRecs.withColumn("date", lit(date)), LAYER_DWD, "UserRecs", "date");

        // Get item factors to calculate movie similarities
        Dataset<Row> itemFactors = model.itemFactors();
        spark.udf().register("calSimilarity", (UDF2<DenseVector, DenseVector, Double>) this::cosineSimilarity, DoubleType);
        spark.udf().register("sortSims", (UDF1<Seq<Row>, List<String>>) this::sortSims, createArrayType(StringType));
        Dataset<RecommendationLog> movieLog = itemFactors.as("a")
                .crossJoin(itemFactors.as("b"))
                .filter("a.id != b.id")
                .select(col("a.id").as("mid"),
                        col("b.id").as("mid1"),
                        array_to_vector(col("a.features")).as("features1"),
                        array_to_vector(col("b.features")).as("features2"))
                .withColumn("similarity", callUDF("calSimilarity", col("features1"), col("features2")))
                .filter("similarity > 0.6")
                .drop("features1", "features2")
                .withColumn("sims", struct(col("mid1").as("mid"), col("similarity")))
                .groupBy("mid")
                .agg(collect_list("sims").as("simList"))
                .withColumn("simList", callUDF("sortSims", col("simList")))
                .as(Encoders.bean(RecommendationLog.class));

        // Write to HBase for fast key based lookup in Streaming recommendation
        writeToHBase(movieLog);

        model.userFactors().unpersist();
        model.itemFactors().unpersist();
    }

    private void writeToHBase(Dataset<RecommendationLog> df) {
        df.foreachPartition(block -> {
            String columnFamily = "info";
            Connection conn = HBaseConnectionUtil.newConnection();
            HBaseClient client = new HBaseClient(conn);

            client.createTableIfNotExist(DB_DATABASE, "MovieSim", columnFamily);
            List<Put> puts = new ArrayList<>();
            while (block.hasNext()) {
                RecommendationLog item = block.next();
                Put put = new Put(Bytes.toBytes(item.mid));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("simList"), Bytes.toBytes(Arrays.toString(item.simList.toArray())));
                puts.add(put);
            }
            client.insertRows(DB_DATABASE, "MovieSim", puts);

            HBaseConnectionUtil.closeConnection(conn);
        });
    }

    private double cosineSimilarity(DenseVector v1, DenseVector v2) {
        return v1.dot(v2) / (Vectors.norm(v1, 2) * Vectors.norm(v2, 2));
    }

    private List<String> sortSims(Seq<Row> simSeq) {
        List<Row> simList = new ArrayList<>(JavaConverters.seqAsJavaListConverter(simSeq).asJava());
        simList.sort(Comparator.comparingDouble(sim -> -(Double) sim.getAs("similarity")));
        ArrayList<String> res = new ArrayList<>();
        simList.forEach(r -> res.add(new Gson().toJson(new Recommendation(r.getAs("mid"), r.getAs("similarity")))));
        return res;
    }

    @Data
    @AllArgsConstructor
    public class Recommendation implements Serializable {
        public int mid;
        public double similarity;
    }
}
