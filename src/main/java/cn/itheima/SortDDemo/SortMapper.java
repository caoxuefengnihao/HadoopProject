package cn.itheima.SortDDemo;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class SortMapper extends Mapper<LongWritable,Text,Text,DoubleWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println(value.toString());
        String string = value.toString();
        String[] split = string.split("\t");
        /*OrderBean orderBean = new OrderBean();
        orderBean.setOrderid(split[0]);
        orderBean.setPrice(Double.parseDouble(split[2]));*/
        context.write(new Text(split[0]),new DoubleWritable(Double.parseDouble(split[2])));
        System.out.println("----------");
    }
}
