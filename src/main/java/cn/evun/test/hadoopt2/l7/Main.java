package cn.evun.test.hadoopt2.l7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.util.Progressable;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URL;

public class Main {
    static {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
        System.setProperty("HADOOP_USER_NAME","hdfs");
    }

    /*public static void main(String[] args) throws IOException {
        InputStream inputStream = new URL("hdfs://hadoop01:9000/wordcount.txt").openStream();
        IOUtils.copyBytes(inputStream,System.out,4096,false);
        IOUtils.closeStream(inputStream);
    }*/
    @Test
    public void test1() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://hadoop01:9000");
        FSDataInputStream open = FileSystem.get(configuration).open(new Path("hdfs://hadoop01:9000/wordcount.txt"));
        IOUtils.copyBytes(open,System.out,4096,false);
        System.out.println("\n\n");
        open.seek(50);
        IOUtils.copyBytes(open,System.out,4096,false);
        IOUtils.closeStream(open);

    }
    @Test
    public void test2() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://hadoop01:9000");
        FileSystem fileSystem = FileSystem.get(configuration);
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("hdfs://hadoop01:9000/user/tzheng/test.txt"));
        byte[] bytes = "zhengtao hello hadoop!...".getBytes();
        fsDataOutputStream.write(bytes,0,bytes.length);
        IOUtils.closeStream(fsDataOutputStream);
    }
    @Test
    public void test3() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://hadoop01:9000");
        FileSystem fileSystem = FileSystem.get(configuration);
        FSDataOutputStream fsDataOutputStream = fileSystem.append(new Path("hdfs://hadoop01:9000/user/tzheng/test.txt"));
        byte[] bytes = "mapreduce is very good for hadle big data!...".getBytes();
        fsDataOutputStream.write(bytes,0,bytes.length);
        IOUtils.closeStream(fsDataOutputStream);
    }
    @Test
    public void test4() throws IOException {
        File file = new File("E:\\mrl\\amazon-meta.txt");
        final long length = file.length()/65536;
        BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(file));
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://hadoop01:9000");
        FileSystem fileSystem = FileSystem.get(configuration);
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("hdfs://hadoop01:9000/user/tzheng/amazon-meta1.txt"), new Progressable() {
            int size = 0;
            public void progress() {
                System.out.println(size++/((double)length));
            }
        });
        IOUtils.copyBytes(bufferedInputStream,fsDataOutputStream,65536,true);


    }
    @Test
    public void test5() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://hadoop01:9000");
        FileSystem fileSystem = FileSystem.get(configuration);
        fileSystem.mkdirs(new Path("hdfs://hadoop01:9000/user/tzheng/test1/test11/test111"));
    }


    @Test
    public void test6() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(out);
        IntWritable i = new IntWritable(163);
        i.write(dout);
        int size = dout.size();
        byte[] bs = new byte[size];
        dout.write(bs,0,size);
        System.out.println(bs);
    }
}
