package pers.haike.demo.hdfsaudit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pers.haike.demo.utils.FileTools;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;

public class LastAccessTime {

    private static Logger log = LoggerFactory.getLogger(LastAccessTime.class);

    private static final String ACCESS_OUTPUT = "ACCESSTIME";

    private static final List<String> PROCESS_CMDS = Arrays.asList(
            "rename","delete", "setReplication", "truncate", "open", "mkdirs", "create", "setTimes");

    private static final String ACCESS_CMD = "access";
    private static final String DELETE_CMD = "delete";

    private static final Charset CHARSET = Charset.forName("UTF-8");

    private static String getAccessOrDeleteCmd(String cmdPart) {
        for (String cmd : PROCESS_CMDS) {
            if (cmdPart.startsWith(cmd)) {
                if (cmd.equals(DELETE_CMD)) {
                    return DELETE_CMD;
                }
                return ACCESS_CMD;
            }
        }
        return "";
    }


    private static class CmdTime implements Writable {

        private String cmd;
        private long time;

        public CmdTime() {
        }

        public CmdTime(String cmd, long time) {
            this.cmd = cmd;
            this.time = time;
        }

        public String getCmd() { return this.cmd; }

        public long getTime() { return this.time; }

        @Override
        public void write(DataOutput out) throws IOException {
            byte[] cmdBytes = cmd.getBytes(CHARSET);
            out.writeInt(cmdBytes.length);
            out.write(cmdBytes);
            out.writeLong(time);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            int cmdLen = in.readInt();
            byte[] cmdBytes = new byte[cmdLen];
            in.readFully(cmdBytes);
            cmd = new String(cmdBytes, CHARSET);
            time = in.readLong();
        }
    }

    public static class MyMapper extends Mapper<LongWritable, Text, Text, CmdTime> {

        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        @Override
        protected void map(LongWritable key, Text tValue, Context context)
            throws IOException, InterruptedException {
            String value = tValue.toString();
            String[] parts = value.split("\\t");
            String timePart = parts[0].substring(0, 19);
            Long time;
            try {
                time = sdf.parse(timePart).getTime();
            } catch (Exception e) {
                log.error("{}", e);
                return;
            }
            String cmd = getAccessOrDeleteCmd(parts[3].substring(4));
            if (cmd.isEmpty()) {
                return;
            }
            String srcPart = parts[4].substring(4);
            context.write(new Text(srcPart), new CmdTime(cmd, time));
        }
    }

    public static class MyCombine extends Reducer<Text, CmdTime, Text, CmdTime> {


        @Override
        protected void reduce(Text key, Iterable<CmdTime> values, Context context)
                throws IOException, InterruptedException {
            long del_time = 0L;
            long access_time = 0L;
            for (CmdTime cmdTime : values) {
                if (cmdTime.getCmd().equals(DELETE_CMD)) {
                    del_time = Math.max(del_time, cmdTime.getTime());
                } else {
                    access_time = Math.max(cmdTime.getTime(), access_time);
                }
            }
            if (del_time >= access_time) {
                context.write(key, new CmdTime(DELETE_CMD, del_time));
            } else {
                context.write(key, new CmdTime(ACCESS_CMD, access_time));
            }
        }
    }

    public static class MyReducer extends Reducer<Text, CmdTime, Text, LongWritable> {

        private MultipleOutputs accessTimeOutput;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            accessTimeOutput = new MultipleOutputs(context);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            accessTimeOutput.close();
        }

        @Override
        protected void reduce(Text key, Iterable<CmdTime> values, Context context)
                throws IOException, InterruptedException {
            long del_time = 0L;
            long access_time = 0L;
            for (CmdTime cmdTime : values) {
                if (cmdTime.getCmd().equals(DELETE_CMD)) {
                    del_time = Math.max(del_time, cmdTime.getTime());
                } else {
                    access_time = Math.max(cmdTime.getTime(), access_time);
                }
            }
            if (del_time >= access_time) {
                log.info("delete file {} {}", key.toString(), del_time);
                return;
            }
            accessTimeOutput.write(ACCESS_OUTPUT, key, new LongWritable(access_time));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(LastAccessTime.class);
        job.setMapperClass(MyMapper.class);
        job.setCombinerClass(MyCombine.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CmdTime.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //job.setInputFormatClass(LzoTextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path("./input/lzo/audit.txt"));

        FileTools.delFile(new File("./output/"));
        FileOutputFormat.setOutputPath(job, new Path("./output/"));
        MultipleOutputs.addNamedOutput(job, ACCESS_OUTPUT,
                TextOutputFormat.class, Text.class, LongWritable.class);

        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
