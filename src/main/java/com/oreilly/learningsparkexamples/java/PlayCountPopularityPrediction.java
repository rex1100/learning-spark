package com.oreilly.learningsparkexamples.java;

/**
 * Created by mitch on 08/12/16.
 */

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vectors;

import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.regression.LabeledPoint;

import java.util.ArrayList;

public class PlayCountPopularityPrediction {

    public static void main(String[] args) throws Exception {
        HotnessPrediction logisticR = new HotnessPrediction();
        String master = args[0];
        logisticR.run(master);
    }

    public static void run(String master) {
        String trainingPath0 = "output/OutputExample/fullData/1990";
        String trainingPath1 = "output/OutputExample/fullData/1991";
        String trainingPath2 = "output/OutputExample/fullData/1992";
        String trainingPath3 = "output/OutputExample/fullData/1993";
        String trainingPath4 = "output/OutputExample/fullData/1994";
        String trainingPath5 = "output/OutputExample/fullData/1995";
        String trainingPath6 = "output/OutputExample/fullData/1996";
        String trainingPath7 = "output/OutputExample/fullData/1997";
        String trainingPath8 = "output/OutputExample/fullData/1998";
        String trainingPath9 = "output/OutputExample/fullData/1999";

        String testPath0 = "output/OutputExample/fullData/2000";
        String testPath1 = "output/OutputExample/fullData/2001";
        String testPath2 = "output/OutputExample/fullData/2002";
        String testPath3 = "output/OutputExample/fullData/2003";
        String testPath4 = "output/OutputExample/fullData/2004";
        String testPath5 = "output/OutputExample/fullData/2005";
        String testPath6 = "output/OutputExample/fullData/2006";
        String testPath7 = "output/OutputExample/fullData/2007";
        String testPath8 = "output/OutputExample/fullData/2008";
        String testPath9 = "output/OutputExample/fullData/2009";

        JavaSparkContext sc = new JavaSparkContext(
                master, "logisticregressionprediction", System.getenv("SPARK_HOME"), System.getenv("JARS"));

        JavaRDD<String> trainingDecade = sc.textFile(trainingPath0)
                .union(sc.textFile(trainingPath1))
                .union(sc.textFile(trainingPath2))
                .union(sc.textFile(trainingPath3))
                .union(sc.textFile(trainingPath4))
                .union(sc.textFile(trainingPath5))
                .union(sc.textFile(trainingPath6))
                .union(sc.textFile(trainingPath7))
                .union(sc.textFile(trainingPath8))
                .union(sc.textFile(trainingPath9));

        JavaRDD<String> testingDecade = sc.textFile(testPath0)
                .union(sc.textFile(testPath1))
                .union(sc.textFile(testPath2))
                .union(sc.textFile(testPath3))
                .union(sc.textFile(testPath4))
                .union(sc.textFile(testPath5))
                .union(sc.textFile(testPath6))
                .union(sc.textFile(testPath7))
                .union(sc.textFile(testPath8))
                .union(sc.textFile(testPath9));

        JavaRDD<LabeledPoint> trainingData = trainingDecade.map(HotnessPrediction::createEnhancedSongInfo)
                .map((EnhancedSongInfo song) -> {
                    double[] tags = song.getArtistTags();
                    double[] points = new double[tags.length + 2];
                    ArrayList<Integer> indexArray = new ArrayList<Integer>();
                    int[] indices = new int[tags.length + 2];

                    for(int i=0; i<tags.length; i++) {
                        points[i] = tags[i];
                        indexArray.add((int)tags[i]);
                    }

                    points[points.length-2] = song.getArtistFamiliarity();
                    indexArray.add(points.length-2);
                    points[points.length-1] = song.getDuration();
                    indexArray.add(points.length-1);

                    for(int i=0; i<indexArray.size(); i++) {
                        indices[i] = indexArray.get(i);
                    }

                    double isHot = 0.0;
                    if(song.getArtistHotttnesss() > 0.80) {
                        isHot = 1.0;
                    }

                    return new LabeledPoint(isHot, Vectors.sparse(2500, indices, points));
                });

        JavaRDD<LabeledPoint> testingData = testingDecade.map(HotnessPrediction::createEnhancedSongInfo).map((EnhancedSongInfo song) -> {
            double[] tags = song.getArtistTags();
            double[] points = new double[tags.length + 2];
            ArrayList<Integer> indexArray = new ArrayList<Integer>();
            int[] indices = new int[tags.length + 2];

            for(int i=0; i<tags.length; i++) {
                points[i] = tags[i];
                indexArray.add((int)tags[i]);
            }

            points[points.length-2] = song.getArtistFamiliarity();
            indexArray.add(points.length-2);
            points[points.length-1] = song.getDuration();
            indexArray.add(points.length-1);

            for(int i=0; i<indexArray.size(); i++) {
                indices[i] = indexArray.get(i);
            }


            double isHot = 0.0;
            if(song.getArtistHotttnesss() > 0.80) {
                isHot = 1.0;
            }

            return new LabeledPoint(isHot, Vectors.sparse(2500, indices, points));
        });

        trainingData.cache();

        final LogisticRegressionModel model = new LogisticRegressionWithLBFGS().setNumClasses(2).run(trainingData.rdd());

        JavaRDD<Tuple2<Object, Object>> predictionAndLabels = testingData.map(
                new Function<LabeledPoint, Tuple2<Object, Object>>() {
                    public Tuple2<Object, Object> call(LabeledPoint p) {
                        return new Tuple2<Object, Object>(model.predict(p.features()), p.label());
                    }
                }
        );

        MulticlassMetrics metrics = new MulticlassMetrics(predictionAndLabels.rdd());
        double accuracy = metrics.weightedPrecision();

        //model.save(sc.sc(), "target/tmp/javaLogisticRegressionWithLBFGSModel");

        System.out.println("Accuracy = " + accuracy);

    }

    public static EnhancedSongInfo createEnhancedSongInfo(String toSplit){
        toSplit = toSplit.replace("[", "");
        toSplit = toSplit.replace("]", "");
        toSplit = toSplit.replace("WrappedArray", "");

        String[] array = toSplit.split(",");
        String[] tags = array[12].split(";");
        String[] terms = array[13].split(";");
        return new EnhancedSongInfo(Integer.parseInt(array[0]),
                array[1],
                array[2],
                array[3],
                array[4],
                array[5],
                array[6],
                array[7],
                Double.parseDouble(array[8]),
                Double.parseDouble(array[9]),
                Double.parseDouble(array[10]),
                Integer.parseInt(array[11]),
                tags,
                terms);
    }
}
