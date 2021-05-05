package DSPPCode.spark.single_table_association.impl;

import DSPPCode.spark.single_table_association.question.SingleTableAssociation;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableHiveIntervalYearMonthObjectInspector;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import java.util.Iterator;

public class SingleTableAssociationImpl extends SingleTableAssociation {

  @Override
  public JavaRDD<Tuple2<String, String>> singleTableAssociation(JavaRDD<String> lines) {
    JavaPairRDD<String, String> table_1 = lines.mapToPair(new PairFunction<String, String, String>() {
      @Override
      public Tuple2<String, String> call(String s) throws Exception {
        String[] row = s.split(" ");
        return new Tuple2<>(row[0], row[1]);
      }
    });

    JavaPairRDD<String, String> table_2 = lines.mapToPair(
        new PairFunction<String, String, String>() {
          @Override
          public Tuple2<String, String> call(String s) throws Exception {
            String[] row = s.split(" ");
            return new Tuple2<>(row[1], row[0]);
          }
        });

    JavaRDD<Tuple2<String, String>> association = table_1.join(table_2).map(
        new Function<Tuple2<String, Tuple2<String, String>>, Tuple2<String, String>>() {
          @Override
          public Tuple2<String, String> call(
              Tuple2<String, Tuple2<String, String>> stringTuple2Tuple2) throws Exception {
            return new Tuple2<>(stringTuple2Tuple2._2._2, stringTuple2Tuple2._2._1);
          }
        });

    return association;
  }
}
