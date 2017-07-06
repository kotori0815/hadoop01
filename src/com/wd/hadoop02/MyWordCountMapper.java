package com.wd.hadoop02;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by wd199 on 2017/7/6.
 */
public class MyWordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString();
        String[] split = s.split(" ");
        for (String s1 : split) {
            context.write(new Text(s1),new IntWritable(1));
        }
    }
}
