package com.example.hdfstest.util;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

public class HdfsUtil {

    private FileSystem fs = null;

    /**
     * 创建连接
     *
     * @throws Exception
     */
    @Before
    public void init() throws Exception {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.119.128:9000/");
        fs = FileSystem.get(new URI("hdfs://192.168.119.128:9000/"), conf, "root");
    }

    /**
     * 快捷上传文件的API
     *
     * @throws IllegalArgumentException
     * @throws IOException
     */
    @Test
    public void testUpload() throws IllegalArgumentException, IOException {

        fs.copyFromLocalFile(new Path("D:\\data\\file.txt"), new Path("/demo.ktr"));
    }

    /**
     * 快捷下载文件的API
     * (win下调用此API如何不配置本地haoop环境就会报异常：
     * null chmod 0644
     * )
     *
     * @throws IllegalArgumentException
     * @throws IOException
     */
    @Test
    public void testDownload() throws IllegalArgumentException, IOException {

        fs.copyToLocalFile(new Path("/demo.ktr"), new Path("src\\main\\resourcesdemo123.ktr"));
    }

    /**
     * 创建目录
     * @throws IllegalArgumentException
     * @throws IOException
     */
    @Test
    public void testMkdir() throws IllegalArgumentException, IOException{

        fs.mkdirs(new Path("/aaa/bbb/ccc"));
    }

    /**
     * 删除目录或者目录
     * 方法的第二个参数 代表是否递归删除，只有删除目录时这个参数有效
     * 如果是删除文件则该参数无效
     * @throws IllegalArgumentException
     * @throws IOException
     */
    @Test
    public void testRm() throws IllegalArgumentException, IOException{

        fs.delete(new Path("/user"), true);
    }

    /**
     * 重命名
     * @throws IllegalArgumentException
     * @throws IOException
     */
    @Test
    public void testRename() throws IllegalArgumentException, IOException{

        fs.rename(new Path("/hadoop-2.4.1.tar.gz"), new Path("/hadoop.tar.gz"));
    }


    /**
     * 遍历hdfs文件和目录
     *
     * @throws FileNotFoundException
     * @throws IllegalArgumentException
     * @throws IOException
     */
    @Test
    public void testList() throws FileNotFoundException, IllegalArgumentException, IOException {

        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus file = listFiles.next();
            System.out.println(file.getPath());
        }
        System.out.println("----------------------------------");
        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : listStatus) {
            System.out.println((fileStatus.isDirectory() ? "-d-  " : "-f-  ") + fileStatus.getPath());
        }
    }


    /**
     * 通过声明流来下载文件
     *
     * @throws IllegalArgumentException
     * @throws IOException
     */
    @Test
    public void testGet() throws IllegalArgumentException, IOException {

        FSDataInputStream is = fs.open(new Path("/demo111.ktr"));
        FileOutputStream os = new FileOutputStream("G:/demo112.ktr");
        IOUtils.copy(is, os);
    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://server201:9000/");
        FileSystem fs = FileSystem.get(new URI("hdfs://server201:9000/"),conf,"root");
        FSDataOutputStream fsDataOutputStream = fs.create(new Path("/demo.ktr"));
        FileInputStream fileInputStream = new FileInputStream("G:/demo.ktr");
        IOUtils.copy(fileInputStream, fsDataOutputStream);
    }
}
