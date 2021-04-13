package DSPPCode.mapreduce.average_score.impl;

import DSPPCode.mapreduce.average_score.question.ScoreMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class ScoreMapperImpl extends ScoreMapper {
    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] data = value.toString().split(",");
        Text key_1 = new Text("Mathematical analysis");
        Text key_2 = new Text("Probability Theory");
        Text key_3 = new Text("Function of Real Variable");
        context.write(key_1, new IntWritable(Integer.parseInt(data[1])));
        context.write(key_2, new IntWritable(Integer.parseInt(data[2])));
        context.write(key_3, new IntWritable(Integer.parseInt(data[3])));
    }
}
