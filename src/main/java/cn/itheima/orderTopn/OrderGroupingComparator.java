package cn.itheima.orderTopn;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderGroupingComparator extends WritableComparator {

    //如果要按照OrderBean对象比较，必须创建一个无参构造器
    public  OrderGroupingComparator(){
        super(OrderBean.class,true);
    }
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean first = (OrderBean) a;
        OrderBean second = (OrderBean) b;
       return first.getOrderid().compareTo(second.getOrderid());
    }
}
