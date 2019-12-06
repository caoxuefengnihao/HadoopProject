package cn.itheima.bean;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements Writable{

    private long upFlow;
    private long dongFlow;
    private long sumFilow;
    public FlowBean() {
    }
    public FlowBean(long upFlow, long dongFlow, long sumFilow) {
        this.upFlow = upFlow;
        this.dongFlow = dongFlow;
        this.sumFilow = sumFilow;
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
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(dongFlow);
        dataOutput.writeLong(sumFilow);
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
         upFlow = dataInput.readLong();
        dongFlow = dataInput.readLong();
        sumFilow = dataInput.readLong();
    }
}
