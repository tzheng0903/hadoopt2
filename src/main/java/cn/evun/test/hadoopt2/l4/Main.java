package cn.evun.test.hadoopt2.l4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Main {
    static class DistinctMapper extends Mapper<LongWritable, Text,Text, NullWritable>{
        private Text outKey = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strs = value.toString().split("\t");
            outKey.set(strs[0]);
            context.write(outKey,NullWritable.get());
        }
    }
    static class DistinctReducer extends Reducer<Text,NullWritable,Text,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key,NullWritable.get());
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
        job.setMapperClass(DistinctMapper.class);
        job.setReducerClass(DistinctReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job,inPath);
        FileOutputFormat.setOutputPath(job,outPath);
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
