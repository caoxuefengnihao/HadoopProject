package cn.itheima.SortDDemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SortApp {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //通过Job这个类来封装本次mr的相关信息  不过这个是一个静态的类
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        //指定本次mrjob jar包的运行主类
        job.setJarByClass(SortApp.class);
        //指定本次mr所用的mapper reduce的类
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReduce.class);
        //指定本次mr mapper阶段的输出 k v 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setNumReduceTasks(2);
        // 指定本次mr 最终输出的k v 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        //指定本次mr 输入路径 和 最终输出结果存放在什么位置
        FileInputFormat.setInputPaths(job,new Path("file:///C:\\Users\\Administrator\\Desktop\\sout.txt"));
        FileOutputFormat.setOutputPath(job,new Path("file:///C:\\Users\\Administrator\\Desktop\\st2"));
        //提交作业
        job.waitForCompletion(true);
    }
}

