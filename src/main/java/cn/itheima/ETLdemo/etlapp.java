package cn.itheima.ETLdemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class etlapp {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //通过Job这个类来封装本次mr的相关信息  不过这个是一个静态的类
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        //指定本次mrjob jar包的运行主类
        job.setJarByClass(cn.itheima.ETLdemo.etlapp.class);
        //指定本次mr所用的mapper reduce的类
        job.setMapperClass(myEtlmapper .class);
        //指定本次mr mapper阶段的输出 k v 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        // 指定本次mr 最终输出的k v 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //指定本次mr 输入路径 和 最终输出结果存放在什么位置
        FileInputFormat.setInputPaths(job,"/dshuji02.txt");
        FileOutputFormat.setOutputPath(job,new Path("/wordcoundresult"));
        //提交作业
        job.waitForCompletion(true);
    }





}




class myEtlmapper extends Mapper<LongWritable,Text,Text,NullWritable>{

    Text t = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String string = value.toString();
        String[] split = string.split("\t");
        StringBuilder sb = new StringBuilder();
        /**
         * 数据清洗的逻辑
         * 首先  我要遍历这个切分开的数组
         */
        if (split.length<8){return;}
        for (int i = 0; i < split.length; i++) {

                if (i<9){

                    if (i == 3){
                        sb.append(split[i].replaceAll(" ","")+"   ");
                    }else{
                        if (i ==8){
                            split[i]=split[i]+" ";
                            sb.append(split[i]);

                        }else {
                            split[i]=split[i]+"    ";
                            sb.append(split[i]);
                        }
                    }
                }else {
                    if (i == split.length-1){
                        sb.append(split[i]);
                    }else {
                        sb.append(split[i]+"&");
                    }
                }
        }
        t.set(sb.toString());
        context.write(t,NullWritable.get());
    }
}



