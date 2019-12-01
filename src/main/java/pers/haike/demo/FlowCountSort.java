package pers.haike.demo;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowCountSort {
    static class FlowCountSortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

        FlowBean bean = new FlowBean();
        Text phone = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            //拿到的是上一个mapreduce程序的输出结果，各手机号和流量信息
            String line = value.toString();
            String[] fields = line.split("\t");
            //获取手机号
            String phonenum = fields[0];
            //获取上行流量
            long upFlow = Long.parseLong(fields[1]);
            //获取下行流量
            long downFlow = Long.parseLong(fields[2]);
            //多次调用map函数时，只创建一个对象
            bean.set(upFlow, downFlow);
            phone.set(phonenum);

//          write时，就将bean对象序列化出去了  reducer那边反序列化回对象  根据bean对象的sumFlow排序
            //map结束后会分发给reduce，默认根据key的hash函数进行分发
            //reduce要实现全局有序，必须只有一个reduce，否则分成多个reduce，只有在每个reduce产生的文件里是有序的
            context.write(bean, phone);
        }
    }


    static class FlowCountSortReducer extends Reducer<FlowBean, Text, Text, FlowBean> {

        //<bean(),phonenum> 相同key的被分为一组，一起执行一次reduce
        //对于key是对象的情况下，不可能有两个对象相同（即使上行流量下行流量都相同），所以每组都只有一条数据
        @Override
        protected void reduce(FlowBean bean, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            context.write(values.iterator().next(), bean);
        }
    }
}
