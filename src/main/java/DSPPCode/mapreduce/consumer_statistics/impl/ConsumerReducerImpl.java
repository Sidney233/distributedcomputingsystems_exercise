package DSPPCode.mapreduce.consumer_statistics.impl;

import DSPPCode.mapreduce.consumer_statistics.question.Consumer;
import DSPPCode.mapreduce.consumer_statistics.question.ConsumerReducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class ConsumerReducerImpl extends ConsumerReducer {

    @Override
    protected void reduce(Text key, Iterable<Consumer> values, Context context)
            throws IOException, InterruptedException {

    }
}
