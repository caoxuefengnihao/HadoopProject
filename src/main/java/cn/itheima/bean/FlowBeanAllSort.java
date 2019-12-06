package cn.itheima.bean;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBeanAllSort implements WritableComparable<FlowBeanAllSort>{
    private String phone;
    private long upFlow;
    private long dongFlow;
    private long sumFilow;
    public FlowBeanAllSort() {
    }

    public FlowBeanAllSort(String phone, long upFlow, long dongFlow, long sumFilow) {
        this.phone = phone;
        this.upFlow = upFlow;
        this.dongFlow = dongFlow;
        this.sumFilow = sumFilow;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public long getUpFlow() {
        return upFlow;
    }
    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }
    public long getDongFlow() {
        return dongFlow;
    }
    public void setDongFlow(long dongFlow) {
        this.dongFlow = dongFlow;
    }
    public long getSumFilow() {
        return sumFilow;
    }
    public void setSumFilow(long sumFilow) {
        this.sumFilow = sumFilow;
    }
    /**
     *
     *
     * 这是序列化方法
     * @param dataOutput
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(phone);
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(dongFlow);
        dataOutput.writeLong(sumFilow);
    }

    @Override
    public String toString() {
        return "FlowBeanAllSort{" +
                "phone='" + phone + '\'' +
                ", upFlow=" + upFlow +
                ", dongFlow=" + dongFlow +
                ", sumFilow=" + sumFilow +
                '}';
    }

    /**
     *
     * 这个是反序列化方法
     * 但是 一定要注意一点  序列化方法写入的顺序 和 反序列化的顺序要一致
     *
     * @param dataInput
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
         phone = dataInput.readUTF();
         upFlow = dataInput.readLong();
        dongFlow = dataInput.readLong();
        sumFilow = dataInput.readLong();
    }

    @Override
    public int compareTo(FlowBeanAllSort o) {
        //按照first的字典排序
        //首先比较第一个字段，如果相等，再比较第二个字段
        int res1 = this.phone.compareTo(o.getPhone());
        if (res1!=0){
            return res1;
        }else{
            //第一个字段相等的时候，比较第二个地段
            int res2 = this.getSumFilow()>o.getSumFilow()?-1:1;
            return res2;
        }
    }
}
