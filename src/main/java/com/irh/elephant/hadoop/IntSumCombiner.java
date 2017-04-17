package com.irh.elephant.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by iritchie on 17-4-17.
 */
class IntSumCombiner<K1, V1, K2, V2> extends Reducer<Text, IntWritable, Text, IntWritable>{
    private IntWritable result = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
        int sum = 0;
        for(IntWritable val : values){
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result);
    }
}
