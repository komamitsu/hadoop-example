package com.komamitsu.hadooptest;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private static LongWritable one = new LongWritable(1);
    private Text text = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer tokenizer = new StringTokenizer(value.toString());

        if (!tokenizer.hasMoreTokens())
            return;
        tokenizer.nextToken(); // day of the week

        if (!tokenizer.hasMoreTokens())
            return;
        String month = tokenizer.nextToken();

        if (!tokenizer.hasMoreTokens())
            return;
        String date = tokenizer.nextToken();

        text.set(month + " " + date);
        context.write(text, one);
    }
}
