package DSPPCode.mapreduce.pagerank.impl;

import DSPPCode.mapreduce.pagerank.question.PageRankMapper;
import DSPPCode.mapreduce.pagerank.question.ReducePageRankWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class PageRankMapperImpl extends PageRankMapper {
    private Map<String, Double> rankTable = new HashMap<>();
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        if (rankTable.isEmpty()) {
            URI uri = context.getCacheFiles()[0];
            FileSystem fs = FileSystem.get(uri, new Configuration());
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(uri))));
            String content;
            while ((content = reader.readLine()) != null) {
                String[] data = content.split(" ");
                rankTable.put(data[0], Double.parseDouble(data[1]));
            }
        }
        String[] pageInfo = value.toString().split(" ");
        double pageRank = rankTable.get(pageInfo[0]);
        int outLink = pageInfo.length - 1;
        ReducePageRankWritable writable;
        writable = new ReducePageRankWritable();
        writable.setData(String.valueOf(pageRank/outLink));
        writable.setTag(ReducePageRankWritable.PR_L);
        for (int i = 1; i < pageInfo.length; i++) {
            context.write(new Text(pageInfo[i]), writable);
        }
        writable = new ReducePageRankWritable();
        writable.setTag(ReducePageRankWritable.PAGE_INFO);
        writable.setData(value.toString());
        context.write(new Text(pageInfo[0]), writable);
    }
}
