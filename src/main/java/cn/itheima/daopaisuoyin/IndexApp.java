package cn.itheima.daopaisuoyin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 所谓的倒排索引 就是一个单词统计的变相开发
 * 只是将每个单词、的后面加上文件的名称然后进行统计
 *
 */
public class IndexApp {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //通过Job这个类来封装本次mr的相关信息  不过这个是一个静态的类
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        //指定本次mrjob jar包的运行主类
        job.setJarByClass(IndexApp.class);
        //指定本次mr所用的mapper reduce的类
        job.setMapperClass(IndextMapper.class);
        job.setReducerClass(IndexReduce.class);
        //指定本次mr mapper阶段的输出 k v 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 指定本次mr 最终输出的k v 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //指定本次mr 输入路径 和 最终输出结果存放在什么位置
        FileInputFormat.setInputPaths(job,"/dshuji02.txt");
        FileOutputFormat.setOutputPath(job,new Path("/wordcoundresult"));
        //提交作业
        job.waitForCompletion(true);
    }
}
class IndextMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /**
         * InputSplit 是一个逻辑切片 是一个接口
         * FileSplit是他的一个子类 实现了父类的 getSplit方法 得到逻辑切片
         * 这个逻辑切片里面能够获得 文件 的路径 路径里面能够获得文件信息
         */
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        String name = inputSplit.getPath().getName();
        String string = value.toString();
        String[] split = string.split("");
        for (String s : split) {
            //将每一个单词发送出去
            context.write(new Text(s+"_"+name),new IntWritable(1));
        }
    }
}

class IndexReduce extends Reducer<Text,IntWritable,Text,IntWritable>{

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int total = 0;
        for (IntWritable value : values) {
            total += value.get();
        }
        context.write(key,new IntWritable(total));
    }
}
