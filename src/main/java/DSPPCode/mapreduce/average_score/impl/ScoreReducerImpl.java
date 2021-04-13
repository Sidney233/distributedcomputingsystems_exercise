package DSPPCode.mapreduce.average_score.impl;

import DSPPCode.mapreduce.average_score.question.ScoreReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class ScoreReducerImpl extends ScoreReducer {
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        int sum = 0;
        for (IntWritable value: values) {
            sum += value.get();
            count += 1;
        }
        sum = sum/count;
        context.write(key, new IntWritable(sum));
    }
}
