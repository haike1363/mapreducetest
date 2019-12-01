package pers.haike.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import pers.haike.demo.utils.FileTools;

import java.io.File;

//https://www.cnblogs.com/52mm/p/p15.html
public class App {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
//      将默认配置文件传给job
        Job job = Job.getInstance(conf);
//      告诉yarn  jar包在哪
        job.setJarByClass(FlowCount.class);
        //指定job要使用的map和reduce
        job.setMapperClass(FlowCount.FlowCountMapper.class);
        job.setReducerClass(FlowCount.FlowCountReducer.class);
//      指定map的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
//      指定最终输出的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
//      job的输入数据所在的目录
//      第一个参数：给哪个job设置
//      第二个参数：输入数据的目录，多个目录用逗号分隔
        FileInputFormat.setInputPaths(job, new Path("./input/"));
//      job的数据输出在哪个目录
        FileTools.delFile(new File("./output/"));
        FileOutputFormat.setOutputPath(job, new Path("./output/"));
        //将jar包和配置文件提交给yarn
//      submit方法提交作业就退出该程序
//      job.submit();
//      waitForCompletion方法提交作业并等待作业执行
//      true表示将作业信息打印出来，该方法会返回一个boolean值，表示是否成功运行
        boolean result = job.waitForCompletion(true);
//      mr运行成功返回true，输出0表示运行成功，1表示失败
        System.exit(result ? 0 : 1);

    }
}
