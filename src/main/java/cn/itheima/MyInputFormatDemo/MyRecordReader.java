package cn.itheima.MyInputFormatDemo;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;

public class MyRecordReader implements RecordReader<NullWritable,Text>{
    @Override
    public boolean next(NullWritable nullWritable, Text text) throws IOException {
        return false;
    }

    @Override
    public NullWritable createKey() {
        return null;
    }

    @Override
    public Text createValue() {
        return null;
    }

    @Override
    public long getPos() throws IOException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public float getProgress() throws IOException {
        return 0;
    }
}
