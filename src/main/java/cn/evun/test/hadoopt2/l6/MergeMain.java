package cn.evun.test.hadoopt2.l6;

import cn.evun.test.hadoopt2.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class MergeMain {
    static class MergeMapper extends Mapper<NullWritable, BytesWritable, Text,BytesWritable>{
        private Text text = new Text();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String name = fileSplit.getPath().getName();
            text.set(name);
        }

        @Override
        protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
            context.write(text,value);
        }
    }
    static class MergeReduce extends Reducer<Text,BytesWritable,Text,BytesWritable>{
        @Override
        protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
            for (BytesWritable value : values) {
                context.write(key,value);
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        String in = "E:\\mrl\\testmerge\\input";
        String out = "E:\\mrl\\testmerge\\output";
        Path inPath = new Path(in);
        Path outPath = new Path(out);
        Utils.deletePath(outPath, configuration);
        Job job = Job.getInstance(configuration, "merge");
        job.setMapperClass(MergeMapper.class);
        job.setReducerClass(MergeReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);
        job.setInputFormatClass(CusFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job,inPath);
        FileOutputFormat.setOutputPath(job,outPath);
        System.exit(job.waitForCompletion(true)?0:1);

    }
}
