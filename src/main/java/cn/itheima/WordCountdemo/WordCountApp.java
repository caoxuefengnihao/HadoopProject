package cn.itheima.WordCountdemo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
/**
 * 这个类就是mr程序运行时的主类，本类中组装了一些程序运行时
 * 所需要的信息
 *
 *都是模板代码 根据实际的业务逻辑 进行组装  即可
 *
 */
public class WordCountApp {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //通过Job这个类来封装本次mr的相关信息  不过这个是一个静态的类
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        //指定本次mrjob jar包的运行主类
        job.setJarByClass(cn.itheima.WordCountdemo.WordCountApp.class);
        //指定本次mr所用的mapper reduce的类
        job.setMapperClass(cn.itheima.WordCountdemo.wordcountmapper.class);
        job.setReducerClass(cn.itheima.WordCountdemo.wordcountreducer.class);
        //指定本次mr mapper阶段的输出 k v 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 指定本次mr 最终输出的k v 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        /*//指定输出文件的压缩格式
        FileOutputFormat.setCompressOutput(job,true);
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);*/
        //指定本次mr 输入路径 和 最终输出结果存放在什么位置
        FileInputFormat.setInputPaths(job,"file:///C:\\Users\\101-01-0192\\Desktop\\000\\w*.txt,file:///C:\\Users\\101-01-0192\\Desktop\\000\\111\\*");
        //FileInputFormat.setInputPaths(job,"file:///C:\\Users\\101-01-0192\\Desktop\\000\\wc1.txt");
        FileOutputFormat.setOutputPath(job,new Path("file:///C:\\Users\\101-01-0192\\Desktop\\wordcoundresult\\niaho"));
        //提交作业
        job.waitForCompletion(true);
    }
}
/**
 * 使用MapReduce开发wordcount应用程序
 *
 *KEYIN,VALUEIN, KEYOUT, VALUEOUT
 * KEYIN 表示数据输入的时候数据类型 在默认的读取数据组件 InputFormat
 * 他的行为是一行一行的读取待处理的数据，读取一行 返回一行给我们的mr程序
 *这种情况下 keyin就表示每一行的起始偏移量 因此数据类型为long类型的
 * VALUEIN 数据输入的 数据类型 在默认的数据组件下 value就表示读取的这一行内容
 * 因此数据类型为string
 * KEYOUT 表示mapper阶段数据输出的数据类型 在本案例当中 输出的key是单词 因此数据类型为
 * string
 * VALUEOUT 表示数据输出时mapper阶段value的数据类型 在本案例当中输出的是in
 * int类型
 *
 * 注意 这里所说的数据类型 都是jdk自带的类型 因此hadoop自己封装了数据类型
 * long --》longwritable string ---》text  integer ---》intwritable
 *
 */
class wordcountmapper extends Mapper<LongWritable,Text,Text,IntWritable>{

    /**
     * 这里就是mapper阶段的具体的业务逻辑实现方法 该方法的调用取决于读取数据的组件有没有
     * 给mapper传入数据 如果有的话 每传入一次就调用一次map方法
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //拿到传入进来的一行内容
        String str = value.toString();
        String[] words = str.split("\\t");
        System.out.println("mapper");
        // 遍历数组 每出现一个单词 就标记一个数字 1
        for (String word : words) {
            //使用mapper程序的上下文 context
            // 作为reduce节点的输入数据
            context.write(new Text(word),new IntWritable(1));
        }


    }
}
/**
 * 这里就是reduce阶段输入的数据可以类型
 */
class wordcountreducer extends Reducer<Text,IntWritable,Text,Text>{
    /**
     * 这里是reduce阶段具体的业务类处理方法
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     *reduce接收所有来自map阶段处理的数据之后，按照key的字典顺序进行
     * 排序
     * 然后按照key是否相同作为一组去调用reduce方法
     * 本方法的key 就是这一组相同的共同key
     * 把这一组的v作为一个迭代器传入我们的reduce方法
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        System.out.println(key.toString());
        Iterator<IntWritable> iterator = values.iterator();
        ArrayList<String> strings = new ArrayList<>();
        while (iterator.hasNext()){
            IntWritable intWritable = iterator.next();
            strings.add(intWritable.toString());
        }
       /* int sum = 0;
        System.out.println("reduce");
        while (iterator.hasNext()){

            IntWritable intWritable = iterator.next();
            sum = sum + intWritable.get();
        }
        context.write(new Text(key),new IntWritable(sum));*/
       context.write(new Text(key),new Text(strings.toString()));
    }
}