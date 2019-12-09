package cn.evun.test.hadoopt2.l6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class CusRecordReader extends RecordReader {

    private BytesWritable bytesWritable = new BytesWritable();
    private FileSplit inputSplit;
    private Configuration configuration ;
    private boolean process = false;

    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        this.inputSplit = (FileSplit) split;
        this.configuration = context.getConfiguration();
    }

    public boolean nextKeyValue() throws IOException, InterruptedException {

        if(!process){
            byte[] bytes = new byte[(int) inputSplit.getLength()];
            FileSystem fs = FileSystem.get(configuration);
            FSDataInputStream inputStream = fs.open(inputSplit.getPath());
            IOUtils.readFully(inputStream,bytes,0,bytes.length);
            bytesWritable.set(bytes,0,bytes.length);
            IOUtils.closeStream(inputStream);
            process = true;
            return true;
        }
        return false;
    }

    public Object getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    public Object getCurrentValue() throws IOException, InterruptedException {
        return this.bytesWritable;
    }

    public float getProgress() throws IOException, InterruptedException {
        return process?1:0;
    }

    public void close() throws IOException {

    }
}
