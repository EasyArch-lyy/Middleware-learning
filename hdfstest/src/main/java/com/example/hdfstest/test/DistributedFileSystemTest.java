package com.example.hdfstest.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 分布式文件系统
 */
public class DistributedFileSystemTest {



    public static void main(String[] args) {
        FileSystem fs = null;
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.119.128:9000");
        try {
            fs = FileSystem.get(new URI("hdfs://192.168.119.128:9000/"), conf, "root");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
