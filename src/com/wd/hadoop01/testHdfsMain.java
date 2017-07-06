package com.wd.hadoop01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by wd199 on 2017/7/5.
 */
public class testHdfsMain {
    @Test
    public void test01() throws IOException {
        Configuration cfg = new Configuration();
        //namenode的所在机器端口号
        cfg.set("fs.defaultFS","hdfs://192.168.40.136:9000");
        FileSystem system = FileSystem.get(cfg);
        Path srcPath = new Path("F:\\1.png");
        Path destPath = new Path("/dir");
        system.copyFromLocalFile(srcPath,destPath);
    }
}
