package DSPPCode.mapreduce.consumer_statistics.impl;

import DSPPCode.mapreduce.consumer_statistics.question.Consumer;
import DSPPCode.mapreduce.consumer_statistics.question.ConsumerReducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.StringWriter;

public class ConsumerReducerImpl extends ConsumerReducer {

    @Override
    protected void reduce(Text key, Iterable<Consumer> values, Context context)
            throws IOException, InterruptedException {
        long sum = 0;
        int count = 0;
        for (Consumer value : values) {
            sum += value.getMoney();
            count += 1;
        }
        String output;
        output = key + "\t" + Integer.toString(count) + "\t" + Long.toString(sum);
        context.write(new Text(output), NullWritable.get());
    }
}
