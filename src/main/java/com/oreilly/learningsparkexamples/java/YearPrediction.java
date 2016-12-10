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
import java.util.HashMap;
import java.util.Map;

public class YearPrediction {

    public static void main(String[] args) throws Exception {
        YearPrediction logisticR = new YearPrediction();
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

        JavaRDD<String> decadeTextData = sc.textFile(trainingPath0)
                .union(sc.textFile(trainingPath1))
                .union(sc.textFile(trainingPath2))
                .union(sc.textFile(trainingPath3))
                .union(sc.textFile(trainingPath4))
                .union(sc.textFile(trainingPath5))
                .union(sc.textFile(trainingPath6))
                .union(sc.textFile(trainingPath7))
                .union(sc.textFile(trainingPath8))
                .union(sc.textFile(trainingPath9))
                .union(sc.textFile(testPath0))
                .union(sc.textFile(testPath1))
                .union(sc.textFile(testPath2))
                .union(sc.textFile(testPath3))
                .union(sc.textFile(testPath4))
                .union(sc.textFile(testPath5))
                .union(sc.textFile(testPath6))
                .union(sc.textFile(testPath7))
                .union(sc.textFile(testPath8))
                .union(sc.textFile(testPath9));

        JavaRDD<LabeledPoint> dataByIndividualYear = decadeTextData.map(HotnessPrediction::createEnhancedSongInfo)
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

                    double yearMapped = 0.0;

                    switch(song.getYear()) {
                        case 1990: yearMapped = 1.0; break;
                        case 1991: yearMapped = 2.0; break;
                        case 1992: yearMapped = 3.0; break;
                        case 1993: yearMapped = 4.0; break;
                        case 1994: yearMapped = 5.0; break;
                        case 1995: yearMapped = 6.0; break;
                        case 1996: yearMapped = 7.0; break;
                        case 1997: yearMapped = 8.0; break;
                        case 1998: yearMapped = 9.0; break;
                        case 1999: yearMapped = 10.0; break;
                        case 2000: yearMapped = 11.0; break;
                        case 2001: yearMapped = 12.0; break;
                        case 2002: yearMapped = 13.0; break;
                        case 2003: yearMapped = 14.0; break;
                        case 2004: yearMapped = 15.0; break;
                        case 2005: yearMapped = 16.0; break;
                        case 2006: yearMapped = 17.0; break;
                        case 2007: yearMapped = 18.0; break;
                        case 2008: yearMapped = 19.0; break;
                        case 2009: yearMapped = 20.0; break;
                    }

                    return new LabeledPoint(yearMapped, Vectors.sparse(2500, indices, points));
                });

        JavaRDD<LabeledPoint> dataGroupedByTwoYears = decadeTextData.map(HotnessPrediction::createEnhancedSongInfo)
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

                    double yearMapped = 0.0;

                    switch(song.getYear()) {
                        case 1990: yearMapped = 1.0; break;
                        case 1991: yearMapped = 1.0; break;
                        case 1992: yearMapped = 2.0; break;
                        case 1993: yearMapped = 2.0; break;
                        case 1994: yearMapped = 3.0; break;
                        case 1995: yearMapped = 3.0; break;
                        case 1996: yearMapped = 4.0; break;
                        case 1997: yearMapped = 4.0; break;
                        case 1998: yearMapped = 5.0; break;
                        case 1999: yearMapped = 5.0; break;
                        case 2000: yearMapped = 6.0; break;
                        case 2001: yearMapped = 6.0; break;
                        case 2002: yearMapped = 7.0; break;
                        case 2003: yearMapped = 7.0; break;
                        case 2004: yearMapped = 8.0; break;
                        case 2005: yearMapped = 8.0; break;
                        case 2006: yearMapped = 9.0; break;
                        case 2007: yearMapped = 9.0; break;
                        case 2008: yearMapped = 10.0; break;
                        case 2009: yearMapped = 10.0; break;
                    }

                    return new LabeledPoint(yearMapped, Vectors.sparse(2500, indices, points));
                });

        JavaRDD<LabeledPoint> trainingDataByIndividualYear = dataByIndividualYear.sample(false, 0.6);
        trainingDataByIndividualYear.cache();
        JavaRDD<LabeledPoint> testingDataByIndividualYear = dataByIndividualYear.subtract(trainingDataByIndividualYear);

        final LogisticRegressionModel dataByYearModel = new LogisticRegressionWithLBFGS().setNumClasses(21).run(trainingDataByIndividualYear.rdd());

        JavaRDD<LabeledPoint> trainingDataByTwoYears = dataGroupedByTwoYears.sample(false, 0.6);
        trainingDataByTwoYears.cache();
        JavaRDD<LabeledPoint> testingDataByTwoYears = dataByIndividualYear.subtract(trainingDataByTwoYears);

        final LogisticRegressionModel dataByTwoYearsModel = new LogisticRegressionWithLBFGS().setNumClasses(11).run(trainingDataByTwoYears.rdd());

        JavaRDD<Tuple2<Object, Object>> predictionAndLabelsByIndividualYear = testingDataByIndividualYear.map(
                new Function<LabeledPoint, Tuple2<Object, Object>>() {
                    public Tuple2<Object, Object> call(LabeledPoint p) {
                        return new Tuple2<Object, Object>(dataByYearModel.predict(p.features()), p.label());
                    }
                }
        );

        JavaRDD<Tuple2<Object, Object>> predictionAndLabelsByTwoYears = testingDataByTwoYears.map(
                new Function<LabeledPoint, Tuple2<Object, Object>>() {
                    public Tuple2<Object, Object> call(LabeledPoint p) {
                        return new Tuple2<Object, Object>(dataByTwoYearsModel.predict(p.features()), p.label());
                    }
                }
        );

        MulticlassMetrics metricsByIndividualYear = new MulticlassMetrics(predictionAndLabelsByIndividualYear.rdd());
        double accuracyByIndividualYear = metricsByIndividualYear.weightedPrecision();

        System.out.println("Accuracy for individual year prediction = " + accuracyByIndividualYear);

        MulticlassMetrics metricsByTwoYears = new MulticlassMetrics(predictionAndLabelsByTwoYears.rdd());
        double accuracyByTwoYears = metricsByTwoYears.weightedPrecision();

        System.out.println("Accuracy for within one year prediction = " + accuracyByTwoYears);

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
