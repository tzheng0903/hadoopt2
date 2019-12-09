package cn.evun.test.hadoopt2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class Utils {
    public static void deletePath(Path path, Configuration configuration) throws IOException {
        FileSystem fileSystem = FileSystem.get(configuration);
        if (fileSystem.exists(path)) {
            fileSystem.delete(path,true);
        }
    }
}
