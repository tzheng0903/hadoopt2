package cn.evun.test.hadoopt2.l2;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class Main {

    private static String cacheFileName = "E:\\mrl\\join\\input\\customer.txt";

    static class MyMapper extends Mapper<LongWritable, Text, NullWritable, Text>{
        private Text outputValue = new Text();
        Map<Integer, Customer> map = null;
        @Override
        protected void setup(Context context) throws IOException {
            FileSystem fs = FileSystem.get(URI.create(cacheFileName),context.getConfiguration());
            FSDataInputStream fsDataInputStream = fs.open(new Path(cacheFileName));
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fsDataInputStream));
            map = new HashMap<Integer, Customer>(10);
            String line;
            while( (line = bufferedReader.readLine())!= null){
                String[] strs = line.split(" ");
                int id = Integer.parseInt(strs[0]);
                map.put(id,new Customer(id,strs[1],strs[2]));
            }
            bufferedReader.close();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strs = value.toString().split(" ");
            int userId = Integer.parseInt(strs[0]);
            Customer customer = map.get(userId);
            StringBuffer stringBuffer = new StringBuffer();

            stringBuffer.append(userId).append(",")
                    .append(customer.getName()).append(",")
                    .append(customer.getPhone()).append(",")
                    .append(strs[1]).append(",")
                    .append(strs[2]).append(",")
                    .append(strs[3]).append(",");
            outputValue.set(stringBuffer.toString());
            context.write(NullWritable.get(),outputValue);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration,"mapedJointest");
        job.addCacheFile(URI.create(cacheFileName));
        FileSystem fs = FileSystem.get( configuration );
        Path des_path = new Path("E:\\mrl\\join\\output");
        if (fs.exists( des_path )) {
            fs.delete( des_path, true );
        }
        FileInputFormat.addInputPath(job,new Path("E:\\mrl\\join\\input\\order.txt"));
        FileOutputFormat.setOutputPath(job,des_path);
        job.setMapperClass(MyMapper.class);
        job.setMapOutputValueClass(NullWritable.class );
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks( 0 );
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
