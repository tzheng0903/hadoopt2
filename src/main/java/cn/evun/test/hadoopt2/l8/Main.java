package cn.evun.test.hadoopt2.l8;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("fs.defaultFS", "hdfs://10.190.35.128:9000");
        configuration.set("hbase.zookeeper.quorum", "10.190.35.130:2181");
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        Job job = Job.getInstance(configuration,"h2base");
        job.setMapperClass(HDFS2HBaseMapper.class);

        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        TableMapReduceUtil.initTableReducerJob(args[1],null,job,null,null,null,null,false);
        job.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job!");
        }

    }
    static class HDFS2HBaseMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
        private final static byte[] cf = Bytes.toBytes("base");
        private final static byte[] clName = Bytes.toBytes("name");
        private final static byte[] clSex = Bytes.toBytes("sex");
        private final static byte[] clAge = Bytes.toBytes("age");
        private final static byte[] clDep = Bytes.toBytes("dep");
        private final static  ImmutableBytesWritable outkey = new ImmutableBytesWritable();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String s = value.toString();
            String[] split = s.split(",");
            Put put = new Put(Bytes.toBytes(split[0]));

            put.addColumn(cf,clName,Bytes.toBytes(split[1]));
            put.addColumn(cf,clSex,Bytes.toBytes(split[2]));
            put.addColumn(cf,clAge,Bytes.toBytes(split[3]));
            put.addColumn(cf,clDep,Bytes.toBytes(split[4]));
            outkey.set(Bytes.toBytes(key.get()));
            context.write(outkey,put);
        }
    }
//    static class HDFS2HBaseReducer extends TableReducer<Text,Text,NullWritable> {
//
//        @Override
//        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            String s = key.toString();
//            String[] split = s.split(",");
//            Put put = new Put(Bytes.toBytes(split[0]));
//
//            put.addColumn(cf,clName,Bytes.toBytes(split[1]));
//            put.addColumn(cf,clSex,Bytes.toBytes(split[2]));
//            put.addColumn(cf,clAge,Bytes.toBytes(split[3]));
//            put.addColumn(cf,clDep,Bytes.toBytes(split[4]));
//
//            context.write(NullWritable.get(),put);
//        }
//    }
}
