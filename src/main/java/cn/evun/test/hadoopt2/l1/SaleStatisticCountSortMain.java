package cn.evun.test.hadoopt2.l1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SaleStatisticCountSortMain {
    static class SaleStatisticCountSortMapper extends Mapper<Object, Text, IntWritable,Text>{
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] strings = value.toString().split("\t");
            context.write(new IntWritable(Integer.valueOf(strings[1])),new Text(strings[0]));
        }
    }
    static class SaleStatisticCountSortReducer extends Reducer<IntWritable,Text,Text,IntWritable>{
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(value,key);
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration,"SaleStatisticCountSort");
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(SaleStatisticCountSortMapper.class);
        job.setReducerClass(SaleStatisticCountSortReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job,new Path("E:\\mrl\\1\\output1"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\mrl\\1\\output2"));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
