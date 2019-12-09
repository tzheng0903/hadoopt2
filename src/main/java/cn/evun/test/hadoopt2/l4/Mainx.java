package cn.evun.test.hadoopt2.l4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Mainx {
    static class MyComparator extends WritableComparator{
        public MyComparator() {
            super(IntWritable.class,true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }
    }
    static class CountSortMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
        private IntWritable outKey = new IntWritable();
        private Text outValue = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strs = value.toString().split("\t");
            outKey.set(Integer.parseInt(strs[1]));
            outValue.set(strs[0]);
            context.write(outKey,outValue);
        }
    }
    static class CountSortReducer extends Reducer<IntWritable, Text,Text,IntWritable>{
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(value,key);
            }
        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String in = "E:\\mrl\\sort\\input";
        Path inPath = new Path(in);
        String out = "E:\\mrl\\sort\\output";
        Path outPath = new Path(out);
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);
        if(fileSystem.exists(outPath)){
            fileSystem.delete(outPath,true);
        }
        Job job = Job.getInstance(configuration, "distinct job");
        job.setSortComparatorClass(MyComparator.class);
        job.setMapperClass(CountSortMapper.class);
        job.setReducerClass(CountSortReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job,inPath);
        FileOutputFormat.setOutputPath(job,outPath);
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
