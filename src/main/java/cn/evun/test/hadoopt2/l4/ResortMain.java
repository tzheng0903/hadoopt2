package cn.evun.test.hadoopt2.l4;

import cn.evun.test.hadoopt2.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ResortMain {

    static class ResortData implements WritableComparable<ResortData>{
        private int upFlow;
        private int downFlow;
        private int sumFlow;

        public ResortData() {
        }

        public ResortData(int upFlow, int downFlow) {
            this.upFlow = upFlow;
            this.downFlow = downFlow;
            this.sumFlow = this.upFlow + this.downFlow;
        }

        public int getUpFlow() {
            return upFlow;
        }

        public void setUpFlow(int upFlow) {
            this.upFlow = upFlow;
        }

        public int getDownFlow() {
            return downFlow;
        }

        public void setDownFlow(int downFlow) {
            this.downFlow = downFlow;
        }

        public int getSumFlow() {
            return sumFlow;
        }

        public void setSumFlow(int sumFlow) {
            this.sumFlow = sumFlow;
        }

        public int compareTo(ResortData o) {
            if(this.upFlow == o.upFlow){
                return this.downFlow = o.downFlow;
            }else{
                return -this.upFlow + o.upFlow;
            }
        }

        public void write(DataOutput out) throws IOException {
            out.writeInt(upFlow);
            out.writeInt(downFlow);
            out.writeInt(sumFlow);
        }

        public void readFields(DataInput in) throws IOException {
            upFlow = in.readInt();
            downFlow = in.readInt();
            sumFlow = in.readInt();
        }

        @Override
        public String toString() {
            return upFlow + "\t" + downFlow +"\t" + sumFlow;
        }
    }
    static class ResortMapper extends Mapper<LongWritable, Text,ResortData,Text>{
        private Text phone = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strs = value.toString().split("\t");
            int up = Integer.parseInt(strs[1]);
            int down = Integer.parseInt(strs[2]);
            phone.set(strs[0]);
            context.write(new ResortData(up,down),phone);
        }
    }
    static class ResortReducer extends Reducer<ResortData,Text,Text,ResortData>{
        @Override
        protected void reduce(ResortData key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(value,key);
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        String in = "E:\\mrl\\sort\\input1";
        String out = "E:\\mrl\\sort\\output1";
        Path inPath = new Path(in);
        Path outPath = new Path(out);
        Utils.deletePath(outPath,configuration);
        Job job = Job.getInstance(configuration,"resort");
        job.setMapperClass(ResortMapper.class);
        job.setReducerClass(ResortReducer.class);
        job.setMapOutputKeyClass(ResortData.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ResortData.class);
        FileInputFormat.addInputPath(job,inPath);
        FileOutputFormat.setOutputPath(job,outPath);
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
