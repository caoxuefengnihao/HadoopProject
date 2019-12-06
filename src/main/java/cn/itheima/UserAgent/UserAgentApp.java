package cn.itheima.UserAgent;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.hive.ql.exec.UDF;
/**
 * UserAgent 的头里面有很多关于浏览器的访问信息
 * 我们通过自定义hiveUDF来解析里面的信息
 * 导入相关Jar包
 * 当hive提供的函数不能够满足业务处理的需要时，此时可以考虑用户自定义的UDF函数
 * 步骤：
 * 1：继承org.apache.hadoop.hive.ql.udf
 * 2：重写evaluate方法
 */
public class UserAgentApp extends UDF{
    public String evaluate(String userAgent){
        UserAgent ua = new UserAgent(userAgent);
        return ua.getOperatingSystem()+"\t"+ua.getBrowser()+"\t"+ua.getBrowserVersion();
    }
}
