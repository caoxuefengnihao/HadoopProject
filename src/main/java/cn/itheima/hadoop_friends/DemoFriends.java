package cn.itheima.hadoop_friends;

import cn.itheima.bean.FriendsBean;
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
public class DemoFriends {
    public static void main(String[] args) throws Exception{
        //通过Job这个类来封装本次mr的相关信息  不过这个是一个静态的类
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        //指定本次mrjob jar包的运行主类
        job.setJarByClass(cn.itheima.hadoop_friends.DemoFriends.class);
        //指定本次mr所用的mapper reduce的类
        job.setMapperClass(cn.itheima.hadoop_friends.DemoMapper.class);
        job.setReducerClass(cn.itheima.hadoop_friends.DemoReduce.class);
        //指定本次mr mapper阶段的输出 k v 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FriendsBean.class);
        // 指定本次mr 最终输出的k v 类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        //指定本次mr 输入路径 和 最终输出结果存放在什么位置
        FileInputFormat.setInputPaths(job,"/friend.txt");
        FileOutputFormat.setOutputPath(job,new Path("/friendResult"));
        //提交作业
        job.waitForCompletion(true);
    }
}
class DemoMapper extends Mapper<LongWritable,Text,Text,FriendsBean>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        FriendsBean friendsBean = new FriendsBean();
        String string = value.toString();
        String[] split = string.split(":");
        friendsBean.setUsername(split[0]);
        friendsBean.setFriends(split[1]);
        context.write(new Text("fridenbean"),friendsBean);
    }
}
class DemoReduce extends Reducer<Text,FriendsBean,NullWritable,Text>{
    @Override
    protected void reduce(Text key, Iterable<FriendsBean> values, Context context) throws IOException, InterruptedException {
        for (FriendsBean value : values) {
            String name = value.getUsername();
            context.write(NullWritable.get(),new Text(name));
        }
    }
}