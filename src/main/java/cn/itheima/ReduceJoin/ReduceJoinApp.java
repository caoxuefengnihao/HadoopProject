package cn.itheima.ReduceJoin;

import cn.itheima.bean.TableBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.Iterator;

public class ReduceJoinApp {




}
/**
 * 这次的MapReduce与以往的不同 这次读的是多个文件
 * 我们需要进行区分
 */
class MapJoin extends Mapper<LongWritable,Text,Text,TableBean>{
    String name;
    TableBean tableBean = new TableBean();
    Text text = new Text();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        //获取读进来的文件名称
        name = inputSplit.getPath().getName();
    }
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String string = value.toString();
        if (name.contains("order")){
            //封装bean对象
            String[] split = string.split("\t");
            tableBean.setId(split[0]);
            tableBean.setAmount(Integer.parseInt(split[2]));
            //在这里要注意一下 如果对象的值为空 那么序列化的时候将会报错 所以我们应该给一个默认值
            tableBean.setPid(split[1]);
            tableBean.setName("");
            tableBean.setFlag("order");
            text.set(split[1]);
        }else{
            String[] strings = string.split("\t");
            tableBean.setPid(strings[0]);
            tableBean.setName(strings[1]);
            tableBean.setFlag("produce");
            tableBean.setAmount(0);
            tableBean.setId("");
            text.set(strings[0]);
        }
        context.write(text,tableBean);
    }
}
class reduceJoin extends Reducer<Text,TableBean,TableBean,NullWritable>{


    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        Iterator<TableBean> iterator = values.iterator();
        while(iterator.hasNext()){



       }
    }
}