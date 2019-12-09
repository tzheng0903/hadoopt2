package cn.evun.test.hadoopt2.l4;

import cn.evun.test.hadoopt2.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyPartitionMain {

    static class MyPartitioner extends Partitioner<Text,Text>{

        public int getPartition(Text text, Text txt1, int numPartitions) {
            return Integer.parseInt(text.toString().substring(0,3))%3;
        }
    }

    static class MyPartitionMapper extends Mapper<LongWritable, Text,Text, Text>{
        private Text txt = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");
            txt.set(split[0]);
            context.write(txt,value);
        }
    }
    static class MyPartitionReducer extends Reducer<Text,Text,Text,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(value,NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        String in = "E:\\mrl\\sort\\input1";
        String out = "E:\\mrl\\sort\\output2";
        Path inPath = new Path(in);
        Path outPath = new Path(out);
        Utils.deletePath(outPath, configuration);
        Job job = Job.getInstance(configuration, "resort");
        job.setPartitionerClass(MyPartitioner.class);
        job.setNumReduceTasks(3);
        job.setMapperClass(MyPartitionMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(MyPartitionReducer.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        FileInputFormat.addInputPath(job,inPath);
        FileOutputFormat.setOutputPath(job,outPath);
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
