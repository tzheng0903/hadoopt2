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

public class SaleStatisticMain {
    static class SaleStatisticMapper extends Mapper<LongWritable, Text,Text, IntWritable>{
        Text msg = new Text("size");
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String str = value.toString();
            String[] strs = str.split(" ");
            Integer integer = Integer.valueOf(strs[3]);
            context.write(msg,new IntWritable(integer));
        }
    }
    static class SaleStatisticReduce extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0 ,avg = 0,count = 0,max = 0,min = Integer.MAX_VALUE;
            for (IntWritable value : values) {
                int size = value.get();
                if(max < size){
                    max = size;
                }
                if(min > size){
                    min = size;
                }
                count++;
                sum += size;
            }
            avg = sum / count;
            context.write(new Text("avg"),new IntWritable(avg));
            context.write(new Text("max"),new IntWritable(max));
            context.write(new Text("min"),new IntWritable(min));
            context.write(new Text("sum"),new IntWritable(sum));
            context.write(new Text("count"),new IntWritable(count));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "sale stat1");
        job.setMapperClass(SaleStatisticMapper.class);
        job.setReducerClass(SaleStatisticReduce.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        FileInputFormat.addInputPath(job,new Path("E:\\mrl\\1\\input"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\mrl\\1\\output"));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
