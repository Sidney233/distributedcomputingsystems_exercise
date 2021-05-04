package DSPPCode.spark.topk_power.impl;

import DSPPCode.spark.topk_power.question.TopKPower;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.python.modules.math;
import scala.Tuple2;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class TopKPowerImpl extends TopKPower {

  @Override
  public int topKPower(JavaRDD<String> lines) {
    JavaRDD<String> sparks = lines.flatMap(
        new FlatMapFunction<String, String>() {
      @Override
      public Iterator<String> call(String s) throws Exception {
        return Arrays.asList(s.split(" ")).iterator();
      }
    });

    JavaPairRDD<String, Integer> boxes = sparks.mapToPair(
        new PairFunction<String, String, Integer>() {
          @Override
          public Tuple2<String, Integer> call(String s) throws Exception {
            return new Tuple2<>(s, 1);
          }
        });

    JavaPairRDD<String, Integer> count = boxes.groupByKey().mapToPair(
        new PairFunction<Tuple2<String, Iterable<Integer>>, String, Integer>() {
          @Override
          public Tuple2<String, Integer> call(
              Tuple2<String, Iterable<Integer>> stringIterableTuple2) throws Exception {
            Integer sum = 0;
            for (Integer i : stringIterableTuple2._2) {
              sum += 1;
            }
            return new Tuple2<>(stringIterableTuple2._1, sum);
          }
        });

    JavaPairRDD<String, Integer> sorted_count =
        count.mapToPair(
        new PairFunction<Tuple2<String, Integer>, Integer, String>() {
          @Override
          public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2)
              throws Exception {
            return new Tuple2<>(stringIntegerTuple2._2, stringIntegerTuple2._1);
          }
        })
        .sortByKey(false)
        .mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
          @Override
          public Tuple2<String, Integer> call(Tuple2<Integer, String> integerStringTuple2)
              throws Exception {
            return new Tuple2<>(integerStringTuple2._2, integerStringTuple2._1);
          }
        });
    System.out.println(sorted_count.take(10));
    List<Tuple2<String, Integer>> top_5 = sorted_count.take(5);
    int sum = 0;
    for (int i = 0; i < 5; i++) {
      sum += math.pow(top_5.get(i)._2, 2);
    }
    return sum;
  }
}
