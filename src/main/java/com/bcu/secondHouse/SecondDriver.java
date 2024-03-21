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
        TextInputFormat.addInputPath(job, new Path("hdfs://node-1:9000/secondhouse"));
        //配置map
        //告知mapper类是哪个
        job.setMapperClass(SecondMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        //3,shuffer


        //4，配置reduce
        //告知reduce类是哪个
        job.setReducerClass(SecondReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        //5，配置结果输出
        job.setOutputFormatClass(TextOutputFormat.class);
        //指定输出结果的地址
        TextOutputFormat.setOutputPath(job, new Path("hdfs://node-1:9000/secondhouse_out"));

        //提交：submit
        boolean b = job.waitForCompletion(true);
        return b ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int status = ToolRunner.run(new Configuration(), new SecondDriver(), args);
        System.exit(status);
    }

}
