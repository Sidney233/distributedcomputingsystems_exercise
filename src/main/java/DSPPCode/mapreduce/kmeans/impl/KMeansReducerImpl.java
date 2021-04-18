package DSPPCode.mapreduce.kmeans.impl;

import DSPPCode.mapreduce.kmeans.question.KMeansReducer;
import org.apache.hadoop.io.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KMeansReducerImpl extends KMeansReducer {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<List<Double>> points = new ArrayList<>();
        for (Text text : values) {
            String value = text.toString();
            List<Double> point = new ArrayList<>();
            for (String s : value.split(",")) {
                point.add(Double.parseDouble(s));
            }
            points.add(point);
        }
        StringBuilder newCenter = new StringBuilder();
        for (int i = 0; i < points.get(0).size(); i++) {
            double sum = 0;
            for (List<Double> data : points) {
                sum += data.get(i);
            }
            newCenter.append(sum / points.size());
            newCenter.append(",");
        }
        context.write(new Text(newCenter.toString()), NullWritable.get());
    }
}
