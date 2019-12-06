package cn.itheima.CompressDemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * hadoop 数据压缩 -->能够提升速度非常有用  Default Gzip Bzip Lzo Snappy
 *  压缩技术能够有效的减少底层的存储系统（HDFS）读写字节数，压缩提高了网络宽带和磁盘io的
 *  效率，在运行mr程序时，io与网络数据传输 shuffle 和margin 需要花费大量的时间，尤其是数据规模很大和工作
 *  负载秘籍的情况下，因此，使用数据压缩显得非常重要
 * 鉴于磁盘io和网络传输是hadoop的宝贵资源，数据压缩对于节省济源最小化磁盘io和网络传输非常有帮助
 * 可以在任意的MapReduce阶段启用压缩
 * 但是 尽管压缩和解压缩对cpu开销不高，但是对其性能的提升和资源的节省并没有代价
 * 注意：采用压缩技术减少了磁盘io但同时增加了cpu的运算负担，所以压缩特性运用得当能够提升性能，
 * 但是运用不当 也可能降低性能
 *
 *
 * 压缩的基本原则
 *  运算密集型的job 少用压缩
 *  io密集型的job 多用压缩
 *
 *  ----------------->map----------------------------->reduce------------------>
 *  hadoop自动检查          如果发现数据量大造成网络传输          压缩技术能够减少要存储的
 *  文件的扩展名             缓慢 应该考虑压缩技术                数据量，因而降低多需要的磁盘
 *                         Lzo snappy                         空间
 *
 *  案例实操
 *
 *  数据的压缩和解压缩 有一个类 CompressionCodec有两个方法可以轻松的压缩和接压缩数据
 *  想要对正在被写入的输出流的数据进行压缩 我们可以使用createOutputStream(fos)
 *  想要对从输入流读取数据而来的数据进行解压缩 则可以调用createInputStream(fis)
 *
 */
public class CompressApp {
    public static void main(String[] args) throws Exception {
        //压缩传来的文件
        compress("....","org.apache.hadoop.io.compress.BZip2codec");
        //解压缩传来的文件
        decompress("......");
    }
    /**
     * 这个方法是用来对传入的而文件进行压缩
     *
     * @param filename 文件路径名
     * @param method 调用的压缩类
     */
    public static void compress(String filename,String method) throws Exception {
        //1:获取输入流
        FileInputStream fis = new FileInputStream(filename);
        Class<?> aClass = Class.forName(method);
        CompressionCodec o = (CompressionCodec) ReflectionUtils.newInstance(aClass, new Configuration());
        //2:获取输出流
        FileOutputStream fos = new FileOutputStream(filename+o.getDefaultExtension());
        CompressionOutputStream outputStream = o.createOutputStream(fos);
        //3:流的对考
        IOUtils.copyBytes(fis,outputStream,1024*1024,false);
        //4:关资源
        outputStream.close();
        fos.close();
        fis.close();
    }
    /**
     *
     * 此方法是用来解压缩的
     * @param filename 需要解压缩的文件路径
     */
    public static void  decompress(String filename) throws IOException {
        //1:检查文件是否能够解压缩  通过
        CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());
        CompressionCodec codec = factory.getCodec(new Path(filename));
        if (codec == null){
            return;
        }
        //2:获取输入流
        CompressionInputStream inputStream = codec.createInputStream(new FileInputStream(filename));
        //3:获取输出流
        FileOutputStream outputStream = new FileOutputStream(filename + ".decompress");
        //4:流的对考
        IOUtils.copyBytes(inputStream,outputStream,1024*1024*5,false);
        //5:关资源
        inputStream.close();
        outputStream.close();
    }
}
