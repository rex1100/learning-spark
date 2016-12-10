package com.oreilly.learningsparkexamples.java;

/**
 * Created by mitch on 08/12/16.
 */

import org.apache.spark.SparkContext;
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
        String trainingPath = "output/OutputExample/1481257251529/2000";
        String testPath = "output/OutputExample/1481257251529/2001";

        JavaSparkContext sc = new JavaSparkContext(
                master, "logisticregressionprediction", System.getenv("SPARK_HOME"), System.getenv("JARS"));

        JavaRDD<LabeledPoint> trainingData = sc.textFile(trainingPath).map(LogisticRegressionPrediction::createSongInfo).map((SongInfo song) -> {
            double[] points = {song.getArtistFamiliarity(), song.getDuration()};
            double isHot = 0.0;
            if(song.getArtistHotttnesss() >= 0.75) {
                isHot = 2.0;
            } else if(song.getArtistHotttnesss() >= 0.50) {
                isHot = 1.0;
            }
            return new LabeledPoint(isHot, Vectors.dense(points));
        });

        JavaRDD<LabeledPoint> testingData = sc.textFile(trainingPath).map(LogisticRegressionPrediction::createSongInfo).map((SongInfo song) -> {
            double[] points = {song.getArtistFamiliarity(), song.getDuration()};
            return new LabeledPoint(0.0, Vectors.dense(points));
        });

        trainingData.cache();


        final LogisticRegressionModel model = new LogisticRegressionWithLBFGS().setNumClasses(3).run(trainingData.rdd());

        JavaRDD<Tuple2<Object, Object>> predictionAndLabels = testingData.map(
                new Function<LabeledPoint, Tuple2<Object, Object>>() {
                    public Tuple2<Object, Object> call(LabeledPoint p) {
                        Double prediction = model.predict(p.features());
                        return new Tuple2<Object, Object>(prediction, p.label());
                    }
                }
        );

        MulticlassMetrics metrics = new MulticlassMetrics(predictionAndLabels.rdd());
        double accuracy = metrics.accuracy();
        System.out.println("Accuracy = " + accuracy);

        model.save(sc.sc(), "target/tmp/javaLogisticRegressionWithLBFGSModel");
        LogisticRegressionModel sameModel = LogisticRegressionModel.load(sc.sc(),
                "target/tmp/javaLogisticRegressionWithLBFGSModel");

    }

    public static SongInfo createSongInfo(String toSplit){
        toSplit = toSplit.replace("[", "");
        toSplit = toSplit.replace("]", "");
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
