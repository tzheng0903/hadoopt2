package cn.evun.test.hadoopt2.l5;

import cn.evun.test.hadoopt2.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CountMain {
    static class CountMapper extends Mapper<Text, IntWritable, IntWritable,Text>{
        @Override
        protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
            context.write(value,key);
        }
    }
    static class CountReducer extends Reducer<IntWritable,Text,Text,IntWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(value,key);
            }
        }
    }
    static class ComparatorTest extends WritableComparator{
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return b.compareTo(a);
        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        String in = "E:\\mrl\\seq\\input";
        String out = "E:\\mrl\\seq\\output";
        Path inPath = new Path(in);
        Path outPath = new Path(out);
        Utils.deletePath(outPath, configuration);
        Job job = Job.getInstance(configuration, "resort");
        job.setSortComparatorClass(ComparatorTest.class);
        job.setMapperClass(CountMapper.class);
        job.setReducerClass(CountReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job,inPath);
        FileOutputFormat.setOutputPath(job,outPath);
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
