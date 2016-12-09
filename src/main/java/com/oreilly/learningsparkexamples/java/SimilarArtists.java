/**
 * Illustrates joining two csv files
 */
package com.oreilly.learningsparkexamples.java;

import au.com.bytecode.opencsv.CSVReader;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.ListUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.io.StringReader;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

public class SimilarArtists {

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
    String csv1 = "files/artists_similar";
    String csv2 = "files/artists_mbtag";
    String csv3 = "files/artists_term";

    SimilarArtists jsv = new SimilarArtists();
    jsv.run(master, csv1, csv2, csv3);
  }

  public static void run(String master,
                         String similar,
                         String mbtag,
                         String term)
          throws Exception {

    JavaSparkContext sc = new JavaSparkContext(
            master, "similarartists", System.getenv("SPARK_HOME"), System.getenv("JARS"));

    SQLContext sqlContext = new SQLContext(sc);

    /* not included right now to let the job run properly */
    JavaRDD<String> csvFile3 = sc.textFile(similar);
    JavaRDD<String> csvFile4 = sc.textFile(mbtag);
    JavaRDD<String> csvFile5 = sc.textFile(term);

    // these all map single lines to a pair of <ArtistId, List<Value>>
    // the value changes based on the csv we read from
    // These then reduce the lists by union so we only have one artistId followed by a list of values
    // Then this turns them into the Artist object RDDs
    JavaRDD<ArtistAndList> artistSimilarRDD = csvFile3
            .mapToPair(string -> {
              String[] array = string.split(",");
              return new Tuple2<>(array[0], Collections.singletonList(array[1]));
            })
            .reduceByKey((x,y) -> ListUtils.union(x,y))
            /**NOTE: this is not going to be pretty, but because of the scala version we're using we have to do this **/
            .map(pair -> new ArtistAndList((String)((Tuple2)pair)._1(),(List<String>)((Tuple2)pair)._2()));

    JavaRDD<ArtistAndList> artistTagRDD = csvFile4
            .mapToPair(string -> {
              String[] array = string.split(",");
              return new Tuple2<>(array[0], Collections.singletonList(array[1]));
            })
            .reduceByKey((x,y) -> ListUtils.union(x,y))
            // see above
            .map(pair -> new ArtistAndList((String)((Tuple2)pair)._1(),(List<String>)((Tuple2)pair)._2()));
    JavaRDD<ArtistAndList> artistTermRDD = csvFile5
            .mapToPair(string -> {
              String[] array = string.split(",");
              return new Tuple2<>(array[0], Collections.singletonList(array[1]));
            })
            .reduceByKey((x,y) -> ListUtils.union(x,y))
            // see above
            .map(pair -> new ArtistAndList((String)((Tuple2)pair)._1(),(List<String>)((Tuple2)pair)._2()));

    // This turns the RDD's into datasets, Datasets allow us to use Sqlesque commands on the data
    Dataset similarArtists = sqlContext.createDataFrame(artistSimilarRDD, ArtistAndList.class);
    Dataset artistTags = sqlContext.createDataFrame(artistTagRDD, ArtistAndList.class);
    Dataset artistTerms = sqlContext.createDataFrame(artistTermRDD, ArtistAndList.class);

    // naming the Cols allows us to use those names as a specification later
    Dataset artistLists =
    similarArtists
            .join(artistTags, similarArtists.col("artistId").equalTo(artistTags.col("artistId")))
            .join(artistTerms, similarArtists.col("artistId").equalTo(artistTerms.col("artistId")))
            .select(similarArtists.col("artistId"),
                    similarArtists.col("valueList").as("similarArtists"),
                    artistTags.col("valueList").as("artistTags"),
                    artistTerms.col("valueList").as("artistTerms"));

    artistLists.toJavaRDD()
            // for some reason this can't handle coalesce idk
            //.coalesce(10)
            .saveAsTextFile("output/artists/"+ Instant.now().toEpochMilli());
  }
}
