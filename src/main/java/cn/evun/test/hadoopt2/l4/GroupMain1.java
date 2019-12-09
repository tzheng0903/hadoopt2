package cn.evun.test.hadoopt2.l4;

import cn.evun.test.hadoopt2.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class GroupMain1 {
    static class Group1Mapper extends Mapper<LongWritable, Text,GroupPair,Text>{
        private Text outKey = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");
            outKey.set(split[1]);
            context.write(new GroupPair(split[0],Double.parseDouble(split[2])),outKey);
        }
    }
//    static class Group1Reducer extends Reducer<Text,GroupPair,Text,GroupPair>{
//        GroupPair groupPair = new GroupPair("",Double.MIN_VALUE);
//        @Override
//        protected void reduce(Text key, Iterable<GroupPair> values, Context context) throws IOException, InterruptedException {
//            for (GroupPair value : values) {
//                if(value.getAmount() > groupPair.getAmount()){
//                    groupPair.setAmount(value.getAmount());
//                    groupPair.setItem(value.getItem());
//                }
//            }
//            context.write(key,groupPair);
//        }
//    }
    static class Group1Reducer extends Reducer<GroupPair,Text,Text,GroupPair>{
        @Override
        protected void reduce(GroupPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(values.iterator().next(),key);
        }
    }
    static class GroupComp extends WritableComparator {
        public GroupComp() {
            super(GroupPair.class,true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            GroupPair a1 = (GroupPair) a;
            GroupPair b1 = (GroupPair) b;
            return a1.orderId.compareTo(b1.orderId);
        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        String in = "E:\\mrl\\sort\\input2";
        String out = "E:\\mrl\\sort\\output3";
        Path inPath = new Path(in);
        Path outPath = new Path(out);
        Utils.deletePath(outPath, configuration);
        Job job = Job.getInstance(configuration, "resort");
        job.setGroupingComparatorClass(GroupComp.class);
        job.setMapperClass(Group1Mapper.class);
        job.setReducerClass(Group1Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(GroupPair.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(GroupPair.class);
        FileInputFormat.addInputPath(job,inPath);
        FileOutputFormat.setOutputPath(job,outPath);
        System.exit(job.waitForCompletion(true)?0:1);
    }

    private static class GroupPair implements WritableComparable<GroupPair> {
        private String orderId;
        private Double amount;
        public GroupPair() {
        }
        public GroupPair(String orderId, Double amount) {
            this.orderId = orderId;
            this.amount = amount;
        }

        public String getOrderId() {
            return orderId;
        }

        public void setOrderId(String orderId) {
            this.orderId = orderId;
        }

        public Double getAmount() {
            return amount;
        }

        public void setAmount(Double amount) {
            this.amount = amount;
        }
        @Override
        public String toString() {
            return orderId + "\t" + amount;
        }

        public int compareTo(GroupPair o) {
            if(this.orderId.equals(o.getOrderId())){
                if(this.amount > o.getAmount())
                    return 1;
                if(this.amount < o.getAmount())
                    return -1;
                return 0;
            }else{
                return this.orderId.compareTo(o.getOrderId());
            }
        }

        public void write(DataOutput out) throws IOException {
            out.writeUTF(this.orderId);
            out.writeDouble(this.amount);
        }

        public void readFields(DataInput in) throws IOException {
            this.orderId = in.readUTF();
            this.amount = in.readDouble();
        }
    }
}
