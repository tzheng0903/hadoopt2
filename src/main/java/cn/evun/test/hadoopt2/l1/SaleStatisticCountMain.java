package cn.evun.test.hadoopt2.l1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SaleStatisticCountMain {
    static class SaleStatisticCountMapper extends Mapper<LongWritable , Text,Text, IntWritable>{
        private IntWritable size = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String str = value.toString();
            String[] strs = str.split(" ");
            size.set(Integer.valueOf(strs[3]));
            context.write(new Text(strs[0]),size);
        }
    }
    static class SaleStatisticCountReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum =0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key,new IntWritable(sum));

        }
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration,"SaleStatisticCount");
        job.setMapperClass(SaleStatisticCountMapper.class);
        job.setReducerClass(SaleStatisticCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job,new Path("E:\\mrl\\1\\input"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\mrl\\1\\output1"));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
