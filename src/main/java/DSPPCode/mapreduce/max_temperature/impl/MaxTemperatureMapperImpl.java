package DSPPCode.mapreduce.max_temperature.impl;

import DSPPCode.mapreduce.max_temperature.question.MaxTemperatureMapper;
import DSPPCode.mapreduce.max_temperature.question.MaxTemperatureReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

public class MaxTemperatureMapperImpl extends MaxTemperatureMapper {
    private static final IntWritable ONE = new IntWritable(1);

    private final Text word = new Text();

    private final Pattern pattern = Pattern.compile("\\W+");

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] data = value.toString().split(" ");
        context.write(new Text(data[0]), new IntWritable(Integer.parseInt(data[1])));
    }
}
