package com.komamitsu.hadooptest;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MyMapperReducerTest {
    MapDriver<LongWritable, Text, Text, LongWritable> mapDriver;
    ReduceDriver<Text, LongWritable, Text, LongWritable> reduceDriver;
    MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable> mapReduceDriver;

    @Before
    public void setUp() throws Exception {
        MyMapper mapper = new MyMapper();
        MyReducer reducer = new MyReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(),
                new Text("Wed Apr 24 23:04:58 2013 [SESSION 2954383360]: Scanning path '/Users/komamitsu/Library/Caches/com.avast.registration/Cache.db' started."));
        mapDriver.withOutput(new Text("Apr 24"), new LongWritable(1));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        reduceDriver.withInput(new Text("Apr 25"), Arrays.asList(new LongWritable(3), new LongWritable(5), new LongWritable(9)));
        reduceDriver.withOutput(new Text("Apr 25"), new LongWritable(17));
        reduceDriver.runTest();
    }

    @Test
    public void testMapperReducer() throws IOException {
        mapReduceDriver.withInput(new LongWritable(),
                new Text("Wed Apr 24 23:04:58 2013 [SESSION 2954383360]: Scanning path '/Users/komamitsu/Library/Caches/com.avast.registration/Cache.db' started."));
        mapReduceDriver.withInput(new LongWritable(),
                new Text("Wed Apr 25 23:04:58 2013 [SESSION 2954383360]: Scanning path '/Users/komamitsu/Library/Caches/com.avast.registration/Cache.db' started."));
        mapReduceDriver.withInput(new LongWritable(),
                new Text("Wed Apr 24 23:04:58 2013 [SESSION 2954383360]: Scanning path '/Users/komamitsu/Library/Caches/com.avast.registration/Cache.db' started."));
        mapReduceDriver.withOutput(new Text("Apr 24"), new LongWritable(2));
        mapReduceDriver.withOutput(new Text("Apr 25"), new LongWritable(1));
        mapReduceDriver.runTest();
    }
}
