package cn.itheima.MyInputFormatDemo;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * 通过自定义inputFormat  实现小文件的合并
 *
 * 具体思路 我们首先看看hadoop的TextFileInputFormat 是怎么实现的
 *
 */

public class MyInputFormat extends FileInputFormat<NullWritable,Text> {

    @Override
    public RecordReader<NullWritable, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return null;
    }
}
