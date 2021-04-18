package DSPPCode.mapreduce.kmeans.impl;

import DSPPCode.mapreduce.kmeans.question.KMeansMapper;
import DSPPCode.mapreduce.kmeans.question.KMeansRunner;
import DSPPCode.mapreduce.kmeans.question.utils.CentersOperation;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.ml.source.libsvm.LibSVMDataSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KMeansMapperImpl extends KMeansMapper {
    private List<List<Double>> centers = new ArrayList<>();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] dimensions;
        List<Double> point = new ArrayList<>();
        int centerIndex = 0;
        double minDistance = Double.MAX_VALUE;
        int iteration = context.getConfiguration().getInt(KMeansRunner.ITERATION, 0);

        if (centers.size() == 0) {
            String centersPath = context.getCacheFiles()[0].toString();
            centers = CentersOperation.getCenters(centersPath, true);
        }

        dimensions = value.toString().split("[,\t]");
        for (int i = 0; i < dimensions.length - 1; i++) {
            point.add(Double.parseDouble(dimensions[i]));
        }

        for (int i = 0; i < centers.size(); i++) {
            double distance = 0;
            List<Double> center = centers.get(i);
            for (int j = 0; j < center.size(); j++) {
                distance += Math.pow(point.get(j) - center.get(j), 2);
            }
            distance = Math.sqrt(distance);
            if (distance < minDistance) {
                minDistance = distance;
                centerIndex = i;
            }
        }

        String pointData = value.toString().split("\t")[0];
        if (KMeansRunner.compareResult) {
            context.write(new Text(pointData), new Text(String.valueOf(centerIndex)));
        } else {
            context.write(new Text(String.valueOf(centerIndex)), new Text(pointData));
        }
    }
}
