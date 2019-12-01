package pers.haike.demo.hdfsaudit;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import pers.haike.demo.utils.FileTools;

public class LastAccessTime {
    public static class MyMapper extends Mapper<LongWritable, String, String, Long> {

        @Override
        protected void map(LongWritable key, String value, Context context)
            throws IOException, InterruptedException {
            System.out.println(value);
            String[] parts = value.split("\\t");
            System.out.println(Arrays.toString(parts));
        }
    }

    public static class MyCombine extends Reducer<String, Long, String, Long> {

    }

    public static class MyReducer extends Reducer<String, Long, String, Long> {

    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(LastAccessTime.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(String.class);
        job.setMapOutputValueClass(Long.class);

        job.setOutputKeyClass(String.class);
        job.setOutputValueClass(Long.class);

        FileInputFormat.setInputPaths(job, new Path("./input/part-0-9-1575066765487.lzo"));

        FileTools.delFile(new File("./output/"));
        FileOutputFormat.setOutputPath(job, new Path("./output/"));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
