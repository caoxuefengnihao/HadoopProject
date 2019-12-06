package cn.itheima;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * Hello world!
 *
 */
public class App {
    public static void main( String[] args ) throws IOException {
        //首先 我们要将linux 的相关信息设置一下  该类对象封装了客户端或者服务器的相关配置
        Configuration c = new Configuration();
        c.set("fs.defaultFS","hdfs://dshuju01:9000");
        System.setProperty("HADOOP_USER_NAME","root");
        FileSystem f =null;
        try {
            // FileSystem 是一个文件系统类 想要获取该类的对象 有一个get方法  这个方法有一个参数 就是
          f = FileSystem.get(c);
        } catch (IOException e) {
            e.printStackTrace();
        }
        //f.create(new Path("/hello"));//创建一个文件夹
       // f.copyToLocalFile(new Path("/hello/int.txt"),new Path("d:"));// 将hdfs分布式文件系统中的 文件下载到客户端
        f.copyFromLocalFile(new Path("d:/liuliang.txt"),new Path("/hello"));
        /*FSDataOutputStream outputStream = f.create(new Path("/123.txt"));
        FileInputStream  fileInputStream = new FileInputStream("d:/123.txt");
        IOUtils.copyBytes(fileInputStream,outputStream,c);*/
         f.close();
    }
}
