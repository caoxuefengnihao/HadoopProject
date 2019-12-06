package cn.itheima.orderTopn;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OrderReducer extends Reducer<OrderBean,NullWritable,OrderBean,NullWritable>{

    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //获取的就是一个订单中金额最大的一条数据
        context.write(key,NullWritable.get());
    }
}
