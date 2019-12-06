package cn.itheima.orderTopn;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderPartition extends Partitioner<OrderBean,NullWritable> {
    @Override
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int numReduceTask) {
        //进行动态分区
        int i = (orderBean.getOrderid().hashCode() & Integer.MAX_VALUE) % numReduceTask;
        return i;
    }
}
