package cn.evun.test.hadoopt2.l5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
//        write();

        read();
    }

    static void read() throws IOException {
        Configuration configuration = new Configuration();
        SequenceFile.Reader reader = new SequenceFile.Reader(configuration, SequenceFile.Reader.file(new Path("D:\\sq.seq")));
        Writable k = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), configuration);
        Writable v = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), configuration);
        while(reader.next(k,v)){
            System.out.println(k + "  " + v);
        }
        reader.close();
    }
    static void write() throws IOException {
        Configuration configuration = new Configuration();
        SequenceFile.Writer writer = SequenceFile.createWriter(configuration, SequenceFile.Writer.file(new Path("D:\\sq.seq")),
                SequenceFile.Writer.keyClass(Text.class),
                SequenceFile.Writer.valueClass(IntWritable.class));
        Text msg = new Text();
        msg.set("a");
        writer.append(msg,new IntWritable(1));
        msg.set("b");
        writer.append(msg,new IntWritable(2));
        msg.set("c");
        writer.append(msg,new IntWritable(3));
        msg.set("d");
        writer.append(msg,new IntWritable(4));
        writer.close();
    }
}
