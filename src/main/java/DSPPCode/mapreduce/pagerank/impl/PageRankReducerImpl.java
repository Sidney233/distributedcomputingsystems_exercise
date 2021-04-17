package DSPPCode.mapreduce.pagerank.impl;

import DSPPCode.mapreduce.pagerank.question.PageRankReducer;
import DSPPCode.mapreduce.pagerank.question.PageRankRunner;
import DSPPCode.mapreduce.pagerank.question.ReducePageRankWritable;

import org.apache.hadoop.io.*;

import java.io.IOException;

public class PageRankReducerImpl extends PageRankReducer {
    private static final double D = 0.85;
    @Override
    public void reduce(Text key, Iterable<ReducePageRankWritable> values, Context context)
            throws IOException, InterruptedException {
        String[] pageInfo = null;
        int totalPage = context.getConfiguration().getInt(PageRankRunner.TOTAL_PAGE, 0);
        int iteration = context.getConfiguration().getInt(PageRankRunner.ITERATION, 0);
        double sum = 0;
        for (ReducePageRankWritable value : values) {
            String tag = value.getTag();
            if (tag.equals(ReducePageRankWritable.PR_L)) {
                sum += Double.parseDouble(value.getData());
            } else if (tag.equals(ReducePageRankWritable.PAGE_INFO)) {
                pageInfo = value.getData().split(" ");
            }
        }
        double pageRank = (1 - D) / totalPage + D * sum;
        if (iteration == PageRankRunner.MAX_ITERATION - 1) {
            context.write(new Text(pageInfo[0] + " " + String.format("%.5f", pageRank)), NullWritable.get());
        } else {
            context.write(new Text(pageInfo[0] + " " + pageRank), NullWritable.get());
        }
    }
}
