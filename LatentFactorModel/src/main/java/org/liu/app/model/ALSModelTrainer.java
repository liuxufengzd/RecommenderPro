package org.liu.app.model;

import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.liu.app.BaseApp;
import org.liu.constant.Constant;

public class ALSModelTrainer extends BaseApp {
    public static void main(String[] args) {
        new ALSModelTrainer().run(args, 4);
    }

    @Override
    public void process(String[] args, SparkSession spark) {
        // Import train/test rating source
        Dataset<Row> rating = TableReader(Constant.LAYER_ODS, "Rating")
                .select("uid", "mid", "score");
        rating.persist();

        // Prepare train dataset and test dataset
        Dataset<Row>[] splitRating = rating.randomSplit(new double[]{0.8, 0.2});
        Dataset<Row> trainData = splitRating[0];
        Dataset<Row> testData = splitRating[1];

        // Train model on the train data for different parameters to find the best parameter set
        int[] ranks = {50, 100, 200, 300};
        double[] regularizationParams = {0.01, 0.1, 1};
        for (int rank : ranks) {
            for (double rp : regularizationParams) {
                ALS als = new ALS()
                        .setUserCol("uid")
                        .setItemCol("mid")
                        .setRatingCol("score")
                        .setMaxIter(10)
                        .setRank(rank)
                        .setRegParam(rp);

                ALSModel model = als.fit(trainData);

                // Evaluate the model by computing the RMSE on the test data
                // Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
                model.setColdStartStrategy("drop");
                Dataset<Row> predictions = model.transform(testData);
                double rmse = new RegressionEvaluator()
                        .setMetricName("rmse")
                        .setLabelCol("score")
                        .setPredictionCol("prediction")
                        .evaluate(predictions);
                System.out.println("rank:" + rank + " regularization:" + rp + " rmse:" + rmse);
            }
        }

        rating.unpersist(false);
    }
}
