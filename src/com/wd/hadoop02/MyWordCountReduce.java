package com.wd.hadoop02;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by wd199 on 2017/7/6.
 */
public class MyWordCountReduce extends Reducer<Text,IntWritable,Text,IntWritable>{
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<IntWritable> iterator = values.iterator();
        int count=0;
        while (iterator.hasNext()){
            IntWritable next = iterator.next();
            int i = next.get();
            count+=i;
        }
        context.write(key,new IntWritable(count));
    }
}
