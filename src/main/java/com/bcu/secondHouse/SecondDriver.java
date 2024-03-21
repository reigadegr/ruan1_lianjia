package com.bcu.secondHouse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.*;

public class SecondDriver extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(super.getConf(), SecondDriver.class.getSimpleName());
        job.setJarByClass(SecondDriver.class);
        job.setInputFormatClass(TextInputFormat.class);
        //指定地址
        TextInputFormat.addInputPath(job,new Path("hdfs://node-1:9000/secondhouse"));
        //配置map

        return 0;
    }


}
