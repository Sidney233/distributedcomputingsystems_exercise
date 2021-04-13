package DSPPCode.mapreduce.consumer_statistics.impl;

import DSPPCode.mapreduce.consumer_statistics.question.Consumer;
import DSPPCode.mapreduce.consumer_statistics.question.ConsumerMapper;
import clojure.lang.Compiler;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class ConsumerMapperImpl extends ConsumerMapper {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] data = value.toString().split("\t");
        boolean isvip = false;
        if (data[3].equals("vip")) {
            isvip = true;
        }
        Consumer consumer = new Consumer(data[0], Integer.parseInt(data[2]), isvip);
        context.write(new Text(data[3]), consumer);
    }
}
