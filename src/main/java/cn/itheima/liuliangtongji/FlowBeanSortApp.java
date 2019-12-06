package cn.itheima.liuliangtongji;

import cn.itheima.bean.FlowBeanSort;
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

/**
 * mapTask 与 reduceTask 均会对数据按照key进行排序，该操作属于hadoop的默认行为
 * 任何的应用程序中的数据均会被排序，而不管逻辑上需不需要
 * 默认的排序时按照字典顺序排序，且实现该排序的方法是快速排序
 *
 *
 * 排序的分类
 * 1：部分排序 MapReduce根据输入记录的键对数据集排序。保证输出的每个文件内部有序 也就是保证每个分区内部有序
 * 2：全排序：最终输出的结果只有一个文件，且文件内部有序，实现方式是只设置一个reduceTask
 * 但是这样非常不好，处理大型文件时效率极低，因为一台机器处理所有的文件，
 * 完全丧失了MapReduce所提供的并行架构
 * 3：辅助排序（GroupingComparator）：在reduce端对key进行分组。应用于
 * 在接收的key为bean对象是，想让一个或几个字段相同的key进入到一个reduce方式时 ，可以采用分组排序
 * 4：二次排序：在自定义的排序过程当中，如果compareTo中的判断条件为两个 即为二次排序
 *
 *
 * 如果我们想要对某个类型进行排序 那么就将哪个类型放到key上，因为 MapReduce默认又一次快速排序 默认顺序是字典顺序
 * 进行排序的
 *
 *
 *
 * 案例成功
 */
public class FlowBeanSortApp {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //通过Job这个类来封装本次mr的相关信息  不过这个是一个静态的类
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        //指定本次mrjob jar包的运行主类
        job.setJarByClass(FlowBeanSortApp.class);
        //指定本次mr所用的mapper reduce的类
        job.setMapperClass(MMapperSort.class);
        job.setReducerClass(MyreduceSort.class);
        //指定本次mr mapper阶段的输出 k v 类型
        job.setMapOutputKeyClass(FlowBeanSort.class);
        job.setMapOutputValueClass(Text.class);
        // 指定本次mr 最终输出的k v 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBeanSort.class);

        //job.setPartitionerClass(mypar.class);
        //指定reducetask的个数 建立分区 实现多台机器处理map的输出结果
        //job.setNumReduceTasks(7);
        //指定本次mr 输入路径 和 最终输出结果存放在什么位置
        FileInputFormat.setInputPaths(job,new Path("file:///C:\\Users\\Administrator\\Desktop\\data\\part-r-00000"));
        FileOutputFormat.setOutputPath(job,new Path("file:///C:\\Users\\Administrator\\Desktop\\dataPartsort"));
        //提交作业
        job.waitForCompletion(true);
    }
}

class MMapperSort extends Mapper<LongWritable,Text,FlowBeanSort,Text>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String string = value.toString();
        String[] split = string.split("\t");
        FlowBeanSort flow = new FlowBeanSort();
        flow.setUpFlow(Long.parseLong(split[1]));
        flow.setDongFlow(Long.parseLong(split[2]));
        flow.setSumFilow(Long.parseLong(split[3]));
        context.write(flow,new Text(split[0]));
    }
}
class MyreduceSort extends Reducer<FlowBeanSort,Text,Text,FlowBeanSort>{
    @Override
    protected void reduce(FlowBeanSort key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(value,key);
        }
    }
}

class mypar extends Partitioner<FlowBeanSort,Text>{
    @Override
    public int getPartition(FlowBeanSort flowBeanSort, Text text, int i) {
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
