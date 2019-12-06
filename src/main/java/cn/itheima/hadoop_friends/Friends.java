package cn.itheima.hadoop_friends;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Set;
public class Friends {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        //通过Job这个类来封装本次mr的相关信息  不过这个是一个静态的类
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        //指定本次mrjob jar包的运行主类
        job.setJarByClass(cn.itheima.hadoop_friends.Friends.class);
        //指定本次mr所用的mapper reduce的类
        job.setMapperClass(cn.itheima.hadoop_friends.MyMapper.class);
        // 指定本次mr 最终输出的k v 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        /**
         * 设置加载缓存文件
         */
        job.addCacheFile(new URI("hdfs://dshuju01:9000/friend.txt"));
        /**
         * 指定reducetask的个数 建立分区 实现多台机器处理map的输出结果 0表示没有reduce阶段 这是mapjoin的前提
         */
        job.setNumReduceTasks(0);
        /**
         * 指定本次mr 输入路径 和 最终输出结果存放在什么位置
         */
        FileInputFormat.setInputPaths(job,new Path("/friend.txt"));
        FileOutputFormat.setOutputPath(job,new Path("/friendResult"));
        //提交作业
        job.waitForCompletion(true);
    }
}
class MyMapper extends Mapper<LongWritable,Text,Text,Text>{
    HashMap<String, String[]> map = new HashMap<>();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        Configuration configuration = new Configuration();
        FileSystem root = FileSystem.get( cacheFiles[0], configuration, "root");
        FSDataInputStream open = root.open(new Path(cacheFiles[0].getPath()));
        String line = null;
        while ((line = open.readLine()) != null){
            String[] split = line.split(":");
            map.put(split[0],split[1].split(","));
        }
        open.close();
    }
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


        String line1;
        String line2="";
        String string = value.toString();
        String[] split = string.split(":");
        String[] split1 = split[1].split(",");
        Set<String> strings = map.keySet();
        for (String s : strings) {
            if (s.equals(split[0])){
                continue;
            }
            else {
                line1 = s+"&"+split[0];
                for (String s1 : split1) {
                    for (String s2 : map.get(s)) {
                        if(s1.equals(s2)){
                            line2 = line2+s2;
                        }
                    }
                }
            }
            context.write(new Text(line1),new Text(line2));
            line1 = null;
            line2 ="";
        }
    }
}