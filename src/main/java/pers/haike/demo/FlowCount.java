package pers.haike.demo;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Hello world!
 */
public class FlowCount {

    static class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context)
                throws IOException, InterruptedException {
//          每一行读进来的数据转化为String类型
            String line = new String(value.getBytes());
            //根据tab分割
            String[] fields = line.split(" ");
            //取出手机号
            String phonenum = fields[1];
            //取出上行流量  将String转为Long
            Long upFlow = Long.parseLong(fields[fields.length - 3]);
            //取出下行流量
            long downFlow = Long.parseLong(fields[fields.length - 2]);
//          把数据发送给reduce
            context.write(new Text(phonenum), new FlowBean(upFlow, downFlow));
        }
    }

    static class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
        //(18989,[bean1,bean2,bean3])

        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context)
                throws IOException, InterruptedException {
            long sum_upflow = 0;
            long sum_downflow = 0;

//          将上行流量和下行流量分别累加

            for (FlowBean bean : values) {
                sum_upflow += bean.getUpFlow();
                sum_downflow += bean.getDownFlow();
            }
            FlowBean resultBean = new FlowBean(sum_upflow, sum_downflow);
            context.write(key, resultBean);
        }
    }

}
