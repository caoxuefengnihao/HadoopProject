package cn.itheima.SortDDemo;



import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReduce extends Reducer<Text,DoubleWritable,Text,DoubleWritable>{
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
    Double d = 0.0;
        for (DoubleWritable value : values) {
            if (value.get()>d){
                d = value.get();
            }
        }
       context.write(key,new DoubleWritable(d));
    }
}
