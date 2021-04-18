package DSPPCode.mapreduce.consumer_statistics.impl;

import DSPPCode.mapreduce.consumer_statistics.question.Consumer;
import DSPPCode.mapreduce.consumer_statistics.question.ConsumerReducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashSet;
import java.util.Set;

public class ConsumerReducerImpl extends ConsumerReducer {

    @Override
    protected void reduce(Text key, Iterable<Consumer> values, Context context)
            throws IOException, InterruptedException {
        Set hashSet = new HashSet();
        long sum = 0;
        String id;
        long count = 0;
        for (Consumer value : values) {
            sum += value.getMoney();
            id = value.getId();
            if (hashSet.contains(id)) {
                continue;
            }
            else {
                hashSet.add(id);
                count += 1;
            }
        }
        String output;
        output = key + "\t" + Long.toString(count) + "\t" + Long.toString(sum);
        context.write(new Text(output), NullWritable.get());
    }
}
