package pers.haike.demo;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

//根据省份分发给不同的reduce程序，其输入数据是map的输出
public class ProvincePartitioner extends Partitioner<Text, FlowBean> {
    public static HashMap<String, Integer> provinceDict = new HashMap<String, Integer>();

    static {
        provinceDict.put("136", 0);
        provinceDict.put("137", 1);
        provinceDict.put("138", 2);
        provinceDict.put("139", 3);
    }

    //返回的是分区号  给哪个reduce
    @Override
    public int getPartition(Text key, FlowBean value, int num_partitioner) {
//      根据手机号前三位分省份，分给不同的reduce
        String phone_num = key.toString().substring(0, 3);
        Integer provinceId = provinceDict.get(phone_num);
        return provinceId == null ? 4 : provinceId;
    }
}