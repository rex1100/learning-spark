/**
 * Illustrates joining two csv files
 */
package com.oreilly.learningsparkexamples.java;

import java.io.StringReader;
import java.time.Instant;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import java.lang.Double;

import scala.Tuple2;

import au.com.bytecode.opencsv.CSVReader;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

public class JoinSongInfo {

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
//      throw new Exception("Usage JoinSongInfo sparkMaster csv1 csv2");
//		}
    String master = args[0];

    // this is going to be hacky, don't kill me
    // relative paths to the folders, if these don't work use absolute paths
    String csv1 = "files/temp_songs";
    String csv2 = "files/train_triplets.txt";
    String csv3 = "files/subset_tracks_per_year.txt";

    JoinSongInfo jsv = new JoinSongInfo();
    jsv.run(master, csv1, csv2, csv3);
  }

  public static void run(String master,
                         String songs,
                         String plays,
                         String years)
          throws Exception {

    JavaSparkContext sc = new JavaSparkContext(
            master, "joinsonginfo", System.getenv("SPARK_HOME"), System.getenv("JARS"));

    SQLContext sqlContext = new SQLContext(sc);

    JavaRDD<SongInfo> songInfoRDD = sc.textFile(songs).map(JoinSongInfo::createSongInfo);
    JavaRDD<String> csvFile2 = sc.textFile(plays);

    /* not included right now to let the job run properly */
    JavaRDD<String> csvFile6 = sc.textFile(years);

    JavaRDD<SongYear> trackYearRDD = csvFile6
            .mapToPair(string -> {
              String[] array = string.split(",");
              return new Tuple2<>(Integer.parseInt(array[0]), array[1]);
            })
            .map(pair -> new SongYear((String)((Tuple2)pair)._2(),(Integer)((Tuple2)pair)._1()));

    Dataset songInfo = sqlContext.createDataFrame(songInfoRDD, SongInfo.class);
    Dataset songYears = sqlContext.createDataFrame(trackYearRDD, SongYear.class);
    Dataset songPlays = sqlContext.createDataFrame(createSongPlaysRDD(csvFile2), SongPlays.class);

    songInfo
            .join(songPlays, songInfo.col("songId").equalTo(songPlays.col("songId")))
            .join(songYears, songYears.col("trackId").equalTo(songInfo.col("trackId")))
            // map the cols in a order that we know
            .select(songInfo.col("trackId"),
                    songInfo.col("title"),
                    songInfo.col("songId"),
                    songInfo.col("release"),
                    songInfo.col("artistId"),
                    songInfo.col("artistMbid"),
                    songInfo.col("artistName"),
                    songInfo.col("duration"),
                    songInfo.col("artistFamiliarity"),
                    songInfo.col("artistHotttnesss"),
                    songYears.col("year"),
                    songPlays.col("playCount"))
            .sort(songPlays.col("playCount"))
            .toJavaRDD()
            .coalesce(1)
            .saveAsTextFile("output/songs/"+ Instant.now().toEpochMilli());
  }

  public static JavaRDD<SongPlays> createSongPlaysRDD(JavaRDD<String> csvFile) {

    // Structure of this file should be UserId \t songId \t Plays
    // we only care about the last two
    JavaPairRDD<String, Integer> songPlaysPair = csvFile.mapToPair(JoinSongInfo::createPlayTuple);

    // merge all play counts for songs with the same id together
    return songPlaysPair
            .reduceByKey((x,y) -> x + y)
            .map(JoinSongInfo::createSongPlays);
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
