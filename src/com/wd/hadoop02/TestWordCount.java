package com.wd.hadoop02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Created by wd199 on 2017/7/6.
 */
public class TestWordCount {
    public static void main(String[] args) {
        try {
            Configuration cfg = new Configuration();
            FileSystem fileSystem = FileSystem.get(cfg);
            Job job = Job.getInstance(cfg);
            job.setJarByClass(TestWordCount.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            Path srcPath = new Path("/src1/a.txt");
            TextInputFormat.addInputPath(job,srcPath);
            Path path = new Path("/result");
            boolean b = fileSystem.exists(path);
            if (b){
                fileSystem.delete(path,true);
            }
            TextOutputFormat.setOutputPath(job,path);

            job.setMapperClass(MyWordCountMapper.class);
            job.setReducerClass(MyWordCountReduce.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
