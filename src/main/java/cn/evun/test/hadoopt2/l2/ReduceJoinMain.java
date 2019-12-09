package cn.evun.test.hadoopt2.l2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 连接客户和订单表
 */
public class ReduceJoinMain {
    //类型1表示客户
    public static final int TYPE_CUSTOMER = 1;
    //类型2表示订单
    public static final int TYPE_ORDER = 2;

    public static class ReduceJoinTag implements Writable{
        private int type;
        private String data;

        public ReduceJoinTag() {
        }

        public ReduceJoinTag(int type, String data) {
            this.type = type;
            this.data = data;
        }

        public int getType() {
            return type;
        }

        public void setType(int type) {
            this.type = type;
        }

        public String getData() {
            return data;
        }

        public void setData(String data) {
            this.data = data;
        }

        public void write(DataOutput out) throws IOException {
            out.writeInt(type);
            out.writeUTF(data);
        }

        public void readFields(DataInput in) throws IOException {
            type = in.readInt();
            data = in.readUTF();
        }
    }

    private static ReduceJoinTag reduceJoinTag = new ReduceJoinTag();
    private static IntWritable mapperOutputKey = new IntWritable(0);

    static class ReduceJoinMapper extends Mapper<LongWritable, Text, IntWritable,ReduceJoinTag>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strs = value.toString().split(" ");
            if(strs.length !=3 & strs.length !=4){
                return ;
            }
            if(strs.length == 3){
                reduceJoinTag.setType(ReduceJoinMain.TYPE_CUSTOMER);
            }else if(strs.length == 4){
                reduceJoinTag.setType(ReduceJoinMain.TYPE_ORDER);
            }
            reduceJoinTag.setData(value.toString());
            mapperOutputKey.set(Integer.parseInt(strs[0]));
            context.write(mapperOutputKey,reduceJoinTag);
        }
    }
    static class ReduceJoinReducer extends Reducer<IntWritable,ReduceJoinTag, NullWritable,Text>{
        private Text reducerOutputValue = new Text();

        @Override
        protected void reduce(IntWritable key, Iterable<ReduceJoinTag> values, Context context) throws IOException, InterruptedException {
            String customerTag = null;
            List<String> reduceJoinTagList = new ArrayList<String>();
            for (ReduceJoinTag reduceJoinTag: values){
                if(reduceJoinTag.getType() == ReduceJoinMain.TYPE_CUSTOMER){
                    customerTag = reduceJoinTag.getData();
                }
                if(reduceJoinTag.getType() == ReduceJoinMain.TYPE_ORDER){
                    reduceJoinTagList.add(reduceJoinTag.getData());
                }
            }
            for (String joinTag : reduceJoinTagList) {
                reducerOutputValue.set(customerTag + " " + joinTag);
                context.write(NullWritable.get(),reducerOutputValue);
            }
        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        String in = "E:\\mrl\\join\\input";
        String out = "E:\\mrl\\join\\output";

        Path inPath = new Path(in);
        Path outPath = new Path(out);

        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(configuration);
        if(fs.exists(outPath)){
            fs.delete(outPath,true);
        }
        Job job = Job.getInstance(configuration,"reducejoinMain test");
        job.setMapOutputValueClass(ReduceJoinTag.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setMapperClass(ReduceJoinMapper.class);
        job.setReducerClass(ReduceJoinReducer.class);
        FileInputFormat.addInputPath(job,inPath);
        FileOutputFormat.setOutputPath(job,outPath);
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
