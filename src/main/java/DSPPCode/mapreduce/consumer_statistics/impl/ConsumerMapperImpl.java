package DSPPCode.mapreduce.consumer_statistics.impl;

import DSPPCode.mapreduce.consumer_statistics.question.ConsumerMapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class ConsumerMapperImpl extends ConsumerMapper {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] data = value.toString().split("\t");
        context.write(new Text(data[3]), IInteger.parseInt(data[2]));
    }
}
