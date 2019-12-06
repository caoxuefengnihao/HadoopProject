package cn.itheima.orderTopn;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderMapper extends Mapper<LongWritable,Text,OrderBean,NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //String str = "Order_0000001\tPdt_01\t222.8";
        String[] split = value.toString().split("\t");
        OrderBean orderBean = new OrderBean();
        orderBean.setOrderid(split[0]);
        orderBean.setPrice(Double.parseDouble(split[2]));
        context.write(orderBean,NullWritable.get());

    }
}
