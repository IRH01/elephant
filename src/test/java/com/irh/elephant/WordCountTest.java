package com.irh.elephant;

import com.irh.elephant.hadoop.IntSumReducer;
import com.irh.elephant.hadoop.TokenizerMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by iritchie on 17-1-16.
 */
public class WordCountTest{

    MapDriver<Object, Text, Text, IntWritable> mapDriver;
    ReduceDriver<Object, Text, Text, IntWritable> reduceDriver;
    MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp(){
        TokenizerMapper mapper = new TokenizerMapper();
        IntSumReducer reducer = new IntSumReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException{
        mapDriver.withInput(new LongWritable(), new Text(""));
        mapDriver.withOutput(new Text("6"), new IntWritable(1));
        mapDriver.runTest();
    }

}
