package cn.itheima.liuliangtongji;

import cn.itheima.bean.FlowBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;

public class tongjiApp {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //通过Job这个类来封装本次mr的相关信息  不过这个是一个静态的类
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        //指定本次mrjob jar包的运行主类
        job.setJarByClass(cn.itheima.liuliangtongji.tongjiApp.class);
        //指定本次mr所用的mapper reduce的类
        job.setMapperClass(cn.itheima.liuliangtongji.myMapper.class);
        job.setReducerClass(cn.itheima.liuliangtongji.myReduce.class);
        //指定本次mr mapper阶段的输出 k v 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        // 指定本次mr 最终输出的k v 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //自定义分区
       // job.setPartitionerClass(MyPartition.class);
        //指定reducetask的个数 建立分区 实现多台机器处理map的输出结果
        //job.setNumReduceTasks(7);
        //指定本次mr 输入路径 和 最终输出结果存放在什么位置
        FileInputFormat.setInputPaths(job,"file:///C:\\Users\\Administrator\\Desktop\\data_flow.txt");
        FileOutputFormat.setOutputPath(job,new Path("file:///C:\\Users\\Administrator\\Desktop\\data"));
        //提交作业
        job.waitForCompletion(true);
    }


}
class myMapper extends Mapper<LongWritable,Text,Text,FlowBean>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        FlowBean flowBean = new FlowBean();
        String line = value.toString();
        String[] split = line.split("\t");
        String phonenumber = split[1];
        flowBean.setDongFlow(Long.parseLong(split[split.length-2]));
        flowBean.setUpFlow(Long.parseLong(split[split.length-3]));
        flowBean.setSumFilow((Long.parseLong(split[split.length-2])+Long.parseLong(split[split.length-3])));
        context.write(new Text(phonenumber),flowBean);
    }
}
class myReduce extends Reducer<Text,FlowBean,Text,Text>{

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long SUMupFlow= 0;
        long SumDownFlow= 0;
        long SumFlow = 0;
        Iterator<FlowBean> iterator = values.iterator();
        while (iterator.hasNext()){
            FlowBean f = iterator.next();
            SUMupFlow+= f.getUpFlow();
            SumDownFlow +=f.getDongFlow();
            SumFlow += f.getSumFilow();
        }
        String line =SUMupFlow +"\t"+SumDownFlow+"\t"+SumFlow;
        context.write(new Text(key),new Text(line));
    }
}

/**
 * 自定义分区 将手机号分开
 *
 */
class MyPartition extends Partitioner<Text,FlowBean>{

    /**
     * 成功
     * @param text 手机号
     * @param flowBean 流量pojo
     * @param i
     * @return 分区数
     */
    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        String string = text.toString();
        if(string.contains("134")){
            return 0;
        }
        if(string.contains("135")){
            return 1;
        }
        if(string.contains("136")){
            return 2;
        }
        if(string.contains("137")){
            return 3;
        }
        if(string.contains("138")){
            return 4;
        }
        if(string.contains("139")){
            return 5;
        }else{

            return 6;
        }
    }
}