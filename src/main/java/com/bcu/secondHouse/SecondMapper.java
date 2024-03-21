package com.bcu.secondHouse;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SecondMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    //定义输出的key,value
    Text outputKey = new Text();
    LongWritable outputValue = new LongWritable(1);

    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] infos = line.split(",");
        String address = infos[3];
        //将地区字符串封装到输出的key中
        outputKey.set(address);
        //使用上下文对象context写出key和value
        context.write(outputKey, outputValue);
    }
}
