package DSPPCode.spark.moving_averages.impl;

import DSPPCode.spark.moving_averages.question.MovingAverages;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MovingAveragesImpl extends MovingAverages {
  int k = 2;
  @Override
  public JavaRDD<String> movingAverages(JavaRDD<String> lines, JavaSparkContext sc) {
    long N = lines.count();
    JavaPairRDD<Integer, Integer> seq = lines.mapToPair(
        new PairFunction<String, Integer, Integer>() {
          @Override
          public Tuple2<Integer, Integer> call(String s) throws Exception {
            String[] data = s.split(",");
            for (int i = 0; i < data.length; i++) {
              data[i] = data[i].replace("[", "");
              data[i] = data[i].replace("]", "");
            }
            return new Tuple2<>(Integer.parseInt(data[0]), Integer.parseInt(data[1]));
          }
        });

    List<JavaPairRDD<Integer, Integer>> other_seqs = new ArrayList<>();

    for (int i = -k; i <= k; i++) {
      if (i == 0) {
        continue;
      }
      int finalI = i;
      JavaPairRDD<Integer, Integer> tmp = seq.mapToPair(
          new PairFunction<Tuple2<Integer, Integer>, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> integerIntegerTuple2)
                throws Exception {
              return new Tuple2<>(integerIntegerTuple2._1 + finalI, integerIntegerTuple2._2);
            }
          });
      other_seqs.add(tmp);
    }

    for (JavaPairRDD<Integer, Integer> i : other_seqs) {
      seq = seq.union(i);
    }

    seq = seq.reduceByKey(new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer integer, Integer integer2) throws Exception {
        return  integer + integer2;
      }
    }).mapToPair(new PairFunction<Tuple2<Integer, Integer>, Integer, Integer>() {
      @Override
      public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> integerIntegerTuple2)
          throws Exception {
        int sum = 0;
        if (integerIntegerTuple2._1 <= k) {
          sum = integerIntegerTuple2._1 + k;
        } else if (N-integerIntegerTuple2._1 < k) {
          sum = (int)N - integerIntegerTuple2._1 + 1 + k;
        } else {
          sum = 2 * k + 1;
        }
        return new Tuple2<>(integerIntegerTuple2._1, integerIntegerTuple2._2/sum);
      }
    });

    JavaRDD<String> result = seq.flatMap(new FlatMapFunction<Tuple2<Integer, Integer>, String>() {
      @Override
      public Iterator<String> call(Tuple2<Integer, Integer> integerIntegerTuple2) throws Exception {
        List<String> res = new ArrayList<>();
        if (integerIntegerTuple2._1 > 0 && integerIntegerTuple2._1 < N+1) {
          res.add("[" + integerIntegerTuple2._1.toString() + "," + integerIntegerTuple2._2.toString() + "]");
        }
        return res.iterator();
      }
    });
    System.out.println(seq.take(100));
    System.out.println(result.take(100));
    JavaRDD<String> res = sc.parallelize(result.take(1000));
    return res;
  }
}
