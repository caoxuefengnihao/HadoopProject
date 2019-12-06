package cn.itheima.MapJoinDemo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
/**
 * 对于join的案例 我们采用ReduceJoin的方式处理起来很慢 我们可不可以去掉reduce阶段 减少shuffle阶段 这样就能
 * 够大大的缩短时间 提高效率
 * 具体做法就是在map端进行join 我们先将小表分布式缓存到各个节点的内存当中 然后在map阶段就开始join
 * 减少shuffle过程
 *
 * 具体的实现步骤：
 * 1 首先 我们先写驱动类 加载需要缓存的数据
 *  job.addCacheFile(new URI("......"))
 * 2 map端join的逻辑不需要Reduce阶段 设置ReduceTask数量为零
 *  job.setNumReduceTasks(0)
 * 3 读取缓存的文件数据
 * setup()方法中
 *    1：获取缓存的文件
 *    2：循环读取缓存文件的一行 进行逻辑上的程序编写
 */
public class DistributedCacheMap {
    public static void main(String[] args) throws Exception {
        //通过Job这个类来封装本次mr的相关信息  不过这个是一个静态的类
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        //指定本次mrjob jar包的运行主类
        job.setJarByClass(cn.itheima.MapJoinDemo.DistributedCacheMap.class);
        //指定本次mr所用的mapper reduce的类
        job.setMapperClass(cn.itheima.MapJoinDemo.DistributedMap.class);
        // 指定本次mr 最终输出的k v 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        /**
         * 设置加载缓存文件
         */
        job.addCacheFile(new URI("........"));
        /**
         * 指定reducetask的个数 建立分区 实现多台机器处理map的输出结果 0表示没有reduce阶段 这是mapjoin的前提
         */
        job.setNumReduceTasks(0);
        /**
         * 指定本次mr 输入路径 和 最终输出结果存放在什么位置
         */
        FileInputFormat.setInputPaths(job,".......");
        FileOutputFormat.setOutputPath(job,new Path("....."));
        //提交作业
        job.waitForCompletion(true);
    }
}
class DistributedMap extends Mapper<LongWritable,Text,Text,NullWritable>{

    HashMap<String,String> map = new HashMap<>();
    Text text = new Text();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        /**
         * 在这里获取到job缓存的数据文件 得到的是一个个URI
         */
        URI[] cacheFiles = context.getCacheFiles();
        String string = cacheFiles[0].getPath().toString();
        /**
         * 运用io流的相关代码 通过路径读取我们的缓存文件
         */
        BufferedReader bufferedReader = new BufferedReader(new FileReader(string));
        String line = null;
        while ((line = bufferedReader.readLine()) != null){
            String[] split = line.split("\t");
            map.put(split[0],split[1]);
        }
        bufferedReader.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit inputSplit = (FileSplit)context.getInputSplit();
        //file name
        String name = inputSplit.getPath().getName();


        String string = value.toString();
        String[] split = string.split("\t");
        String s = map.get(split[1]);
        String lines = null;
        if(s != null) {
         lines = split[0] + "\t" + s + "\t" + split[2];
         }
         if (lines != null){
             text.set(lines);
             context.write(text,NullWritable.get());
         }

    }
}
