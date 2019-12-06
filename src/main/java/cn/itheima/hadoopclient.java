package cn.itheima;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.net.URI;
/**
 * 用java来操作hdfs
 *
 *
 *
 */

public class hadoopclient {
    private static String PATH = "hdfs://dshuju01:9000";
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        FileSystem root = FileSystem.get(new URI(PATH), configuration, "root");
        /**
         *1 创建文件夹
         */
        root.mkdirs(new Path("/hello"));
        /**
         *
         * 2 创建文件
         * 注意  在创建文件的时候 其返回值是一个 FSDataOutputStream 字节流输出流
         * 这样我们可以写入一些数据在创建的文件里
         */
        //root.create(new Path("/dshuji01.txt"));
        //FSDataOutputStream outputStream = root.create(new Path("/wordcount.txt"));
        //outputStream.write("hello hadoop hello spark hello java\nhello jjj hello kkk hello bbb\nggg ggg hhh iii ooo".getBytes());
        /**
         *
         * 3 重命名
         */
        //root.rename(new Path("/123.txt"),new Path("/1234.txt"));
        /**
         *
         * 4 从本地将文件copy到hdfs 采用IO的方式
         */
       // FSDataOutputStream output = root.copy
        /**
         *
         * 5 查看hdfs上某个文件的内容
         */
       // FSDataInputStream open = root.open(new Path("/dshuji02.txt"));
       // IOUtils.copyBytes(open,System.out,1024);
        /**
         * 6 查看某个目录下的所有文件
         */
       /* FileStatus[] fileStatuses = root.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            System.out.println(fileStatus.getPath());
        }*/
    }
}
