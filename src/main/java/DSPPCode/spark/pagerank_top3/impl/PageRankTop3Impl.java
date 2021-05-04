package DSPPCode.spark.pagerank_top3.impl;

import DSPPCode.spark.pagerank_top3.question.PageRankTop3;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class PageRankTop3Impl extends PageRankTop3 {

  @Override
  public JavaPairRDD<String, Double> getTop3(JavaRDD<String> text, int iterateNum, JavaSparkContext sc) {
    double q = 0.85;
    long N = text.count();
    JavaPairRDD<String, List<String>> links =
        text.mapToPair(new PairFunction<String, String, List<String>>() {
          @Override
          public Tuple2<String, List<String>> call(String s) throws Exception {
            String[] link = s.split(" ");
            List<String> result = new ArrayList<>();
            for (int i = 2; i < link.length; i = i + 2) {
              result.add(link[i]);
            }
            return new Tuple2<String, List<String>>(link[0], result);
          }
        });
    System.out.println("links: " + links.take(100));
    JavaPairRDD<String, Double> ranks =
        text.mapToPair(new PairFunction<String, String, Double>() {
          @Override
          public Tuple2<String, Double> call(String s) throws Exception {
            String[] data = s.split(" ");
            double rank = Double.parseDouble(data[1]);
            return new Tuple2<String, Double>(data[0], rank);
          }
        });
    System.out.println("ranks:" + ranks.take(100));
    for(int i = 0; i < iterateNum; i++) {
      JavaPairRDD<String, Double> contributions =
          links.join(ranks).flatMapToPair(
              new PairFlatMapFunction<Tuple2<String, Tuple2<List<String>, Double>>, String, Double>() {
                @Override
                public Iterator<Tuple2<String, Double>> call(
                    Tuple2<String, Tuple2<List<String>, Double>> characterTuple2Tuple2)
                    throws Exception {
                  List<Tuple2<String, Double>> result = new ArrayList<>();
                  double rate = characterTuple2Tuple2._2._2/characterTuple2Tuple2._2._1.size();
                  for (int i = 0; i < characterTuple2Tuple2._2._1.size(); i++) {
                    result.add(new Tuple2<String, Double>(characterTuple2Tuple2._2._1.get(i), rate));
                  }
                  return result.iterator();
                }
              });

      ranks = contributions.reduceByKey(new Function2<Double, Double, Double>() {
        @Override
        public Double call(Double aDouble, Double aDouble2) throws Exception {
          return aDouble + aDouble2;
        }}).mapToPair(new PairFunction<Tuple2<String, Double>, String, Double>() {
        @Override
        public Tuple2<String, Double> call(Tuple2<String, Double> characterDoubleTuple2)
            throws Exception {
          return new Tuple2<>(characterDoubleTuple2._1, (1 - q) / N + q * characterDoubleTuple2._2);
        }
      });
      // System.out.println("new_ranks" + ranks.take(100));
    }
    ranks = ranks
        .mapToPair(new PairFunction<Tuple2<String, Double>, Double, String>() {
          @Override
          public Tuple2<Double, String> call(Tuple2<String, Double> characterDoubleTuple2)
              throws Exception {
            return new Tuple2<>(characterDoubleTuple2._2, characterDoubleTuple2._1);
          }
        })
        .sortByKey(false)
        .mapToPair(new PairFunction<Tuple2<Double, String>, String, Double>() {
          @Override
          public Tuple2<String, Double> call(Tuple2<Double, String> doubleCharacterTuple2)
              throws Exception {
            return new Tuple2<>(doubleCharacterTuple2._2, doubleCharacterTuple2._1);
          }
        });
    List<Tuple2<String, Double>> top_3 = ranks.take(3);

    JavaPairRDD<String, Double> result = sc.parallelizePairs(top_3);
    System.out.println(result.take(100));
    return result;
  }
}
