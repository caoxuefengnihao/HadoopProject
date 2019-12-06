package cn.itheima.orderTopn;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {

    private String orderid;
    private Double price;

    @Override
    public String toString() {
        return orderid + "\t" +price;
    }

    public String getOrderid() {
        return orderid;
    }

    public void setOrderid(String orderid) {
        this.orderid = orderid;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }



    //订单id相同的情况下进行金额的比较
    @Override
    public int compareTo(OrderBean o) {
        //订单id相同
        int i = this.orderid.compareTo(o.orderid);
        if(i==0){
            //进行金额的比较
            i = this.price.compareTo(o.price);
        }
        return -i;
    }
    //序列化
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.orderid);
        out.writeDouble(this.price);
    }
    //反序列化
    @Override
    public void readFields(DataInput in) throws IOException {
        this.orderid = in.readUTF();
        this.price = in.readDouble();
    }
}
