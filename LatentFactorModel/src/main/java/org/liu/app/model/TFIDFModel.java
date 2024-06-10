package org.liu.app.model;

import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.ml.linalg.SparseVector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.liu.app.BaseApp;
import org.liu.constant.Constant;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.DoubleType;

/**
 * Content-based recommender
 */
public class TFIDFModel extends BaseApp {
    public static void main(String[] args) {
        new TFIDFModel().run(args, 4);
    }

    @Override
    public void process(String[] args, SparkSession spark) {
        // Base on Genres
//        Dataset<Row> source = TableReader(Constant.LAYER_DIM, "Movie")
//                .select("mid", "name", "genres")
//                .na().drop(new String[]{"mid", "name", "genres"})
//                .filter("genres != ''");
//
//        Dataset<Row> df = source.withColumn("genres", regexp_replace(col("genres"), "\\|", " "));
//
//        // Term Frequency - Inverse Document Frequency
//        spark.udf().register("calSimilarity", (UDF2<SparseVector, SparseVector, Double>) this::cosineSimilarity, DoubleType);
//        df = new Tokenizer()
//                .setInputCol("genres")
//                .setOutputCol("words")
//                .transform(df);
//
//        df = new HashingTF()
//                .setInputCol("words")
//                .setOutputCol("tf")
//                .transform(df);
//
//        IDFModel model = new IDF()
//                .setInputCol("tf")
//                .setOutputCol("featureVector").fit(df);
//
//        df = model.transform(df)
//                .drop("words", "tf");
//
//        df = df.as("a").crossJoin(df.as("b"))
//                .filter("a.mid != b.mid")
//                .select(col("a.mid").as("mid1"),
//                        col("b.mid").as("mid2"),
//                        col("a.featureVector").as("f1"),
//                        col("b.featureVector").as("f2"),
//                        col("a.genres").as("genres1"),
//                        col("b.genres").as("genres2"))
//                .withColumn("similarity", callUDF("calSimilarity", col("f1"), col("f2")))
//                .drop("f1", "f2");

        // Based on description
        Dataset<Row> source = TableReader(Constant.LAYER_DIM, "Movie")
                .select("mid", "name", "description", "genres")
                .na().drop(new String[]{"mid", "name", "description"})
                .filter(trim(col("description")).notEqual(lit("")));

        // Term Frequency - Inverse Document Frequency
        spark.udf().register("calSimilarity", (UDF2<SparseVector, SparseVector, Double>) this::cosineSimilarity, DoubleType);
        Dataset<Row> df = new Tokenizer()
                .setInputCol("description")
                .setOutputCol("words")
                .transform(source);

        df = new HashingTF()
                .setInputCol("words")
                .setOutputCol("tf")
                .transform(df);

        IDFModel model = new IDF()
                .setInputCol("tf")
                .setOutputCol("featureVector").fit(df);

        df = model.transform(df)
                .drop("description", "words", "tf");

        df = df.as("a").crossJoin(df.as("b"))
                .filter("a.mid != b.mid")
                .select(col("a.mid").as("mid1"),
                        col("b.mid").as("mid2"),
                        col("a.featureVector").as("f1"),
                        col("b.featureVector").as("f2"),
                        col("a.genres").as("genres1"),
                        col("b.genres").as("genres2"))
                .withColumn("similarity", callUDF("calSimilarity", col("f1"), col("f2")))
                .drop("f1", "f2");

        df.show(10000, false);
    }

    private double cosineSimilarity(SparseVector v1, SparseVector v2) {
        return v1.dot(v2) / (Vectors.norm(v1, 2) * Vectors.norm(v2, 2));
    }
}
