package cn.evun.test.hadoopt2.l3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class Main {
    /**
     * mapper    单词:文件  -》1
     */
    static class IndexConvertMapper extends Mapper<LongWritable, Text,Text, Text>{
        private Text one = new Text("1");
        private Text mapperKey = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Path path = ((FileSplit) context.getInputSplit()).getPath();
            StringTokenizer st = new StringTokenizer(value.toString());
            while(st.hasMoreTokens()){
                String s = st.nextToken();
                mapperKey.set(s + ":" + path.getName());
                context.write(mapperKey,one);
            }
        }

    }
    static class IndexConvertCombiner extends Reducer<Text,Text,Text,Text>{
        private Text outKey = new Text();
        private Text outValue = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (Text value : values) {
                sum += Integer.parseInt(value.toString());
            }
            String[] wordFile = key.toString().split(":");
            outKey.set(wordFile[0]);
            outValue.set(wordFile[1]+":" + sum);
            context.write(outKey,outValue);

        }
    }
    static class IndexConvertReducer extends Reducer<Text,Text,Text,Text>{
        private Text outValue = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer sb = new StringBuffer();
            sb.append("{ ");
            for (Text value : values) {
                sb.append(value.toString());
                sb.append(" ");
            }
            sb.append(" }");
            outValue.set(sb.toString());
            context.write(key,outValue);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        String in = "E:\\mrl\\search\\input";
        Path inPath = new Path(in);
        String out = "E:\\mrl\\search\\output";
        Path outPath = new Path(out);
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);
        if(fileSystem.exists(outPath)){
            fileSystem.delete(outPath,true);
        }
        Job job = Job.getInstance(configuration);
        job.setMapperClass(IndexConvertMapper.class);
        job.setCombinerClass(IndexConvertCombiner.class);
        job.setReducerClass(IndexConvertReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job,inPath);
        FileOutputFormat.setOutputPath(job,outPath);
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
