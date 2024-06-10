package org.liu.app;

import com.google.gson.Gson;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;
import org.json.JSONArray;
import org.json.JSONObject;
import org.liu.bean.RatingDetail;
import org.liu.bean.RecommendDetail;
import org.liu.client.HBaseClient;
import org.liu.util.HBaseConnectionUtil;
import org.liu.util.RedisUtil;
import org.liu.util.Utils;
import redis.clients.jedis.Jedis;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.*;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.liu.constant.Constant.*;

public class RecommendApp extends BaseApp {
    private Connection conn;

    public static void main(String[] args) {
        new RecommendApp().runStream(args, 4);
    }

    @Override
    public void process(String[] args, SparkSession spark) {
        Dataset<Row> rating = KafkaReader(Topic_Rating);

        // Parse Json file
        StructType schema = new StructType()
                .add("uid", IntegerType)
                .add("mid", IntegerType)
                .add("score", DoubleType);
        rating = rating.select(from_json(col("value"), schema).as("value"))
                .select(col("value.uid").as("uid"),
                        col("value.mid").as("mid"),
                        col("value.score").as("score"))
                .na().drop(new String[]{"uid", "mid", "score"});

        try {
            rating.writeStream()
                    .trigger(Trigger.ProcessingTime(5000))
                    .option("checkpointLocation", Utils.getTableCheckpointPath(LAYER_ADS, "Rating"))
                    .foreachBatch((batch, batch_id) -> {
                        batch.foreachPartition(block -> {
                            Jedis redisClient = RedisUtil.getClient();
                            List<RecommendDetail> recommendDetails = new ArrayList<>();

                            while (block.hasNext()) {
                                Row row = block.next();
                                int uid = row.getAs("uid");
                                int mid = row.getAs("mid");
                                double score = row.getAs("score");

                                // Get at most recent K rating records -> stored in Redis
                                String key = "uid-" + uid;
                                String arrStr;
                                ArrayList<String> ratingList = new ArrayList<>(); // list is used to update redis
                                Gson gson = new Gson();
                                ratingList.add(gson.toJson(new RatingDetail(mid, score)));
                                HashMap<Integer, Double> ratingMap = new HashMap<>();
                                if (redisClient.exists(key)) {
                                    arrStr = redisClient.get(key);
                                    JSONArray jsonArray = new JSONArray(arrStr);
                                    int K = Math.min(FETCH_RECENT_RATING_NUMBER, jsonArray.length());
                                    for (int i = 0; i < K; i++) {
                                        JSONObject jo = jsonArray.getJSONObject(i);
                                        if (ratingList.size() < FETCH_RECENT_RATING_NUMBER)
                                            ratingList.add(gson.toJson(new RatingDetail(jo.getInt("mid"), jo.getDouble("score"))));
                                        ratingMap.put(jo.getInt("mid"), jo.getDouble("score"));
                                    }
                                }
                                redisClient.set(key, Arrays.toString(ratingList.toArray()));

                                // Get top N most similar items
                                JSONArray jsonArray = getSimilarMovies(mid, redisClient);
                                ArrayList<Integer> movies = new ArrayList<>();
                                int N = Math.min(FETCH_MOVIE_NUMBER, jsonArray.length());
                                int current = 0;
                                // Exclude movies rated recently
                                while (N > 0 && current < jsonArray.length()) {
                                    int candidate = jsonArray.getJSONObject(current++).getInt("mid");
                                    if (!ratingMap.containsKey(candidate)) {
                                        movies.add(candidate);
                                        N--;
                                    }
                                }

                                // Calculate recommendation degree for each movie candidate
                                for (Integer movie : movies) {
                                    JSONArray similarMovies = getSimilarMovies(movie, redisClient);
                                    double totalValue = 0;
                                    int totalNum = 0;
                                    int positiveNum = 0;
                                    int negativeNum = 0;
                                    for (int i = 0; i < similarMovies.length(); i++) {
                                        JSONObject jo = similarMovies.getJSONObject(i);
                                        if (ratingMap.containsKey(jo.getInt("mid"))) {
                                            Double ratingScore = ratingMap.get(jo.getInt("mid"));
                                            totalValue += jo.getDouble("similarity") * ratingScore;
                                            totalNum++;
                                            if (ratingScore > 3) positiveNum++;
                                            else negativeNum++;
                                        }
                                    }
                                    positiveNum = Math.max(positiveNum, 1);
                                    negativeNum = Math.max(negativeNum, 1);

                                    double degree = totalNum > 0 ? totalValue / totalNum + Math.log(positiveNum) - Math.log(negativeNum) : 0;
                                    if (degree > 0) recommendDetails.add(new RecommendDetail(uid, movie, degree));
                                }
                            }

                            // Write result to JDBC table
                            // Cannot create DataFrame within foreachBatch, which is nested.
                            if (!recommendDetails.isEmpty()) {
                                String url = "jdbc:mysql://" + DB_HOST + "/" + DB_DATABASE + "?useSSL=false&allowPublicKeyRetrieval=true&useUnicode=true&characterEncoding=utf-8";
                                Properties properties = new Properties();
                                properties.put("driver", DB_DRIVER);
                                properties.put("user", DB_USERNAME);
                                properties.put("password", DB_PWD);
                                java.sql.Connection conn = DriverManager.getConnection(url, properties);
                                String sql = "INSERT INTO t_movie_recommendation (uid, mid, degree) VALUES (?, ?, ?)";
                                PreparedStatement statement = conn.prepareStatement(sql);
                                for (RecommendDetail detail : recommendDetails) {
                                    statement.setInt(1, detail.uid);
                                    statement.setInt(2, detail.mid);
                                    statement.setDouble(3, detail.degree);
                                    statement.executeUpdate();
                                }
                                statement.close();
                                conn.close();
                            }

                            RedisUtil.closeClient(redisClient);
                            HBaseConnectionUtil.closeConnection(conn);
                        });
                    }).start();
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    private JSONArray getSimilarMovies(int mid, Jedis redisClient) {
        String arrStr;
        String key = "mid-" + mid;
        if (!redisClient.exists(key)) {
            if (conn == null) conn = HBaseConnectionUtil.newConnection();
            HBaseClient hBaseClient = new HBaseClient(conn);
            arrStr = hBaseClient.getColumn(DB_DATABASE, "MovieSim", Bytes.toBytes(mid), "info", "simList");
            // Expire in redis after 24h, because the MovieSim is recalculated by the ML model once a day
            redisClient.setex(key, 24 * 60 * 60, arrStr);
        } else {
            arrStr = redisClient.get(key);
        }
        return new JSONArray(arrStr);
    }
}
