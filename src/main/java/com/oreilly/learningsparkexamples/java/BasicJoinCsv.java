/**
 * Illustrates joining two csv files
 */
package com.oreilly.learningsparkexamples.java;

import java.io.StringReader;
import java.time.Instant;

import antlr.collections.List;
import javassist.runtime.Desc;
import org.apache.commons.collections.ListUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import java.lang.Double;
import java.util.Collections;

import scala.Tuple2;

import au.com.bytecode.opencsv.CSVReader;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

public class BasicJoinCsv {

  public static class ParseLine implements PairFunction<String, Integer, String[]> {
    public Tuple2<Integer, String[]> call(String line) throws Exception {
      CSVReader reader = new CSVReader(new StringReader(line));
      String[] elements = reader.readNext();
      Integer key = Integer.parseInt(elements[0]);
      return new Tuple2(key, elements);
    }
  }

  public static void main(String[] args) throws Exception {
//		if (args.length != 3) {
//      throw new Exception("Usage BasicJoinCsv sparkMaster csv1 csv2");
//		}
    String master = args[0];

    // this is going to be hacky, don't kill me
    // relative paths to the folders, if these don't work use absolute paths
    String csv1 = "files/temp_songs";
    String csv2 = "files/train_triplets.txt";
    String csv3 = "files/artists_similar";
    String csv4 = "files/artists_mbtag";
    String csv5 = "files/artists_term";

    BasicJoinCsv jsv = new BasicJoinCsv();
    jsv.run(master, csv1, csv2, csv3, csv4, csv5);
  }

  public static void run(String master, String songs, String plays, String similar, String mbtag, String term)
          throws Exception {

    JavaSparkContext sc = new JavaSparkContext(
            master, "basicjoincsv", System.getenv("SPARK_HOME"), System.getenv("JARS"));

    SQLContext sqlContext = new SQLContext(sc);

    JavaRDD<SongInfo> songInfoRDD = sc.textFile(songs).map(BasicJoinCsv::createSongInfo);
    JavaRDD<String> csvFile2 = sc.textFile(plays);

    /* not included right now to let the job run properly */
    /*
    JavaRDD<String> csvFile3 = sc.textFile(similar);
    JavaRDD<String> csvFile4 = sc.textFile(mbtag);
    JavaRDD<String> csvFile5 = sc.textFile(term);

    JavaPairRDD artistSimilar = csvFile3
            .mapToPair(string -> {
              String[] array = string.split(",");
              return new Tuple2<>(array[0], Collections.singletonList(array[1]));
            })
            .reduceByKey((x,y) -> ListUtils.union(x,y));

    JavaPairRDD artistTag = csvFile4
            .mapToPair(string -> {
              String[] array = string.split(",");
              return new Tuple2<>(array[0], Collections.singletonList(array[1]));
            })
            .reduceByKey((x,y) -> ListUtils.union(x,y));

    JavaPairRDD artistTerm = csvFile5
            .mapToPair(string -> {
              String[] array = string.split(",");
              return new Tuple2<>(array[0], Collections.singletonList(array[1]));
            })
            .reduceByKey((x,y) -> ListUtils.union(x,y));*/

    Dataset songInfo = sqlContext.createDataFrame(songInfoRDD, SongInfo.class);
    Dataset songPlays = sqlContext.createDataFrame(createSongPlaysRDD(csvFile2), SongPlays.class);

    songInfo
            .join(songPlays, songInfo.col("songId").equalTo(songPlays.col("songId")))
            .sort(songPlays.col("playCount"))
            .toJavaRDD()
            .map(row->row.toString())
            .coalesce(1)
            .saveAsTextFile("output/"+ Instant.now().toEpochMilli());
  }

  public static JavaRDD<SongPlays> createSongPlaysRDD(JavaRDD<String> csvFile) {

    // Structure of this file should be UserId \t songId \t Plays
    // we only care about the last two
    JavaPairRDD<String, Integer> songPlaysPair = csvFile.mapToPair(BasicJoinCsv::createPlayTuple);

    // merge all play counts for songs with the same id together
    return songPlaysPair
            .reduceByKey((x,y) -> x + y)
            .map(BasicJoinCsv::createSongPlays);
  }


  public static Tuple2<String, Integer> createPlayTuple(String toSplit) {
    String[] array = toSplit.split("\t");
    return new Tuple2<>(array[1], Integer.parseInt(array[2]));
  }

  public static SongPlays createSongPlays(Tuple2<String,Integer> pair){
    return new SongPlays(pair._1(), pair._2());
  }

  public static SongInfo createSongInfo(String toSplit){
    String[] array = toSplit.split(",");
    return new SongInfo(array[0],
            array[1],
            array[2],
            array[3],
            array[4],
            array[5],
            array[6],
            Double.parseDouble(array[7]),
            array[8],
            Double.parseDouble(array[9]),
            Integer.parseInt(array[10]));
  }
}
