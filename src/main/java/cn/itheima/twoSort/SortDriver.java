package cn.itheima.twoSort;

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

public class SortDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //通过Job这个类来封装本次mr的相关信息  不过这个是一个静态的类
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        //指定本次mrjob jar包的运行主类
        job.setJarByClass(SortDriver.class);
        //指定本次mr所用的mapper reduce的类
        job.setMapperClass(TwoSortMapper.class);
        job.setReducerClass(TwoSortReduce.class);
        //指定本次mr mapper阶段的输出 k v 类型
        job.setMapOutputKeyClass(IntPair.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(1);
        // 指定本次mr 最终输出的k v 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //指定本次mr 输入路径 和 最终输出结果存放在什么位置
        FileInputFormat.setInputPaths(job,new Path("C:\\Users\\101-01-0192\\Desktop\\twosort.txt"));
        FileOutputFormat.setOutputPath(job,new Path("C:\\Users\\101-01-0192\\Desktop\\rel"));
        //提交作业
        job.waitForCompletion(true);
    }
}


class TwoSortMapper extends Mapper<LongWritable,Text,IntPair, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] s = value.toString().split(" ");
        IntPair intPair = new IntPair();
        intPair.setFirst(Integer.parseInt(s[0]));
        intPair.setSecond(Integer.parseInt(s[1]));
        context.write(intPair,NullWritable.get());
    }
}

class TwoSortReduce extends Reducer<IntPair,NullWritable,Text,NullWritable>{

    @Override
    protected void reduce(IntPair key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(new Text(key.getFirst()+" "+key.getSecond()),NullWritable.get());
    }
}