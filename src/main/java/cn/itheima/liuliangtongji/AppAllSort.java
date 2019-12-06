package cn.itheima.liuliangtongji;

import cn.itheima.bean.FlowBeanAllSort;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class AppAllSort {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //通过Job这个类来封装本次mr的相关信息  不过这个是一个静态的类
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        //指定本次mrjob jar包的运行主类
        job.setJarByClass(AppAllSort.class);
        //指定本次mr所用的mapper reduce的类
        job.setMapperClass(SotrMapper.class);
        job.setReducerClass(SortREduce.class);
        //指定本次mr mapper阶段的输出 k v 类型
        job.setMapOutputKeyClass(FlowBeanAllSort.class);
        job.setMapOutputValueClass(NullWritable.class);
        // 指定本次mr 最终输出的k v 类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(FlowBeanAllSort.class);
        //自定义分区
        // job.setPartitionerClass(MyPartition.class);
        //指定reducetask的个数 建立分区 实现多台机器处理map的输出结果
        //job.setNumReduceTasks(0);
        //指定本次mr 输入路径 和 最终输出结果存放在什么位置
        FileInputFormat.setInputPaths(job,"file:///C:\\Users\\Administrator\\Desktop\\ddddd.txt");
        FileOutputFormat.setOutputPath(job,new Path("file:///C:\\Users\\Administrator\\Desktop\\data00"));
        //提交作业
        job.waitForCompletion(true);
    }



}


class SotrMapper extends Mapper<LongWritable,Text,FlowBeanAllSort,NullWritable>{


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        FlowBeanAllSort flowBean = new FlowBeanAllSort();
        String line = value.toString();
        String[] split = line.split("\t");
        String phonenumber = split[0];
        flowBean.setPhone(phonenumber);
        flowBean.setDongFlow(Long.parseLong(split[1]));
        flowBean.setUpFlow(Long.parseLong(split[2]));
        flowBean.setSumFilow((Long.parseLong(split[3])));
        context.write(flowBean,NullWritable.get());

    }
}

class SortREduce extends Reducer<FlowBeanAllSort,NullWritable,NullWritable,FlowBeanAllSort>{

    @Override
    protected void reduce(FlowBeanAllSort key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        context.write(NullWritable.get(),key);

    }
}
