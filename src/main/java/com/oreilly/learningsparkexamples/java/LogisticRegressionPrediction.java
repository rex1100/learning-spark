package com.oreilly.learningsparkexamples.java;

/**
 * Created by mitch on 08/12/16.
 */

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;

import java.util.ArrayList;
import java.util.List;

public class LogisticRegressionPrediction {

    public static void main(String[] args) throws Exception {
        LogisticRegressionPrediction logisticR = new LogisticRegressionPrediction();
        String master = args[0];
        logisticR.run(master);
    }

    public static void run(String master) {
        String path = "output/OutputExample/1481257251529/1927";
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaLogisticRegressionSummaryExample")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(
                master, "logisticregressionprediction", System.getenv("SPARK_HOME"), System.getenv("JARS"));

        SQLContext sqlContext = new SQLContext(sc);

        JavaRDD<String> data = sc.textFile(path);

        JavaRDD<LabeledPoint> songInfoRDD = sc.textFile(path).map(LogisticRegressionPrediction::createSongInfo).map((SongInfo song) -> {
            double[] points = {song.getArtistFamiliarity(), song.getDuration()};
            return new LabeledPoint(song.getArtistHotttnesss(), Vectors.dense(points));
        });


        final LogisticRegressionWithLBFGS model = new LogisticRegressionWithLBFGS();
        model.setNumClasses(10);

        model.run(songInfoRDD.rdd());

    }

    public static SongInfo createSongInfo(String toSplit){
        String[] array = toSplit.split(",");
        return new SongInfo(array[1],
                array[2],
                array[3],
                array[4],
                array[5],
                array[6],
                array[7],
                Double.parseDouble(array[8]),
                Double.parseDouble(array[9]),
                Double.parseDouble(array[10]),
                Integer.parseInt(array[0]));
    }
}
