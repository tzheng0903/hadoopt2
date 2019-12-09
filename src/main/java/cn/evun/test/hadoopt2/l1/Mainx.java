package cn.evun.test.hadoopt2.l1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Mainx {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration,"word count job");
        job.setJarByClass(Main.class);
        job.setMapperClass(Mainx.WordCountMapper.class);
        job.setReducerClass(Mainx.WordCountReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
//        FileInputFormat.addInputPath(job,new Path("E:\\mrl\\part-r-00000"));
//        FileOutputFormat.setOutputPath(job,new Path("E:\\mrl\\outputx5"));
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        System.exit(job.waitForCompletion(true)?0:1);
    }

    private static class WordCountMapper extends Mapper<LongWritable,Text,LongWritable,Text> {
        private Text test = new Text();
        private LongWritable size = new LongWritable();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] s = value.toString().split("\t");
            size.set(Long.parseLong(s[1]));
            test.set(s[0]);
            context.write(size,test);
        }
    }

    private static class WordCountReduce extends Reducer<LongWritable,Text,Text,LongWritable> {
        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(value,key);
            }
        }
    }

}
