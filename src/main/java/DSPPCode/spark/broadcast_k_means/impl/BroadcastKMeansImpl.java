package DSPPCode.spark.broadcast_k_means.impl;
import DSPPCode.spark.broadcast_k_means.question.BroadcastKMeans;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.python.modules.math;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BroadcastKMeansImpl extends BroadcastKMeans{

  @Override
  public Integer closestPoint(List<Integer> p, Broadcast<List<List<Double>>> kPoints) {
    double min = Double.MAX_VALUE;
    int index = -1;
    for (List<Double> center : kPoints.value()) {
      double distance = math.pow(p.get(0) - center.get(0), 2)
          + math.pow(p.get(1) - center.get(1), 2);
      if (distance < min) {
        min = distance;
        index = kPoints.value().indexOf(center);
      }
    }
    return index;
  }

  @Override
  public Broadcast<List<List<Double>>> createBroadcastVariable(JavaSparkContext sc,
      List<List<Double>> localVariable) {
    return sc.broadcast(localVariable);
  }
}
