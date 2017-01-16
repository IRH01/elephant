package com.irh.elephant.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * Created by iritchie on 17-1-11.
 */
public class HDFSTest{

    @Test
    public void testQuery() throws IOException{
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
        String path = "hdfs://192.168.105.9:9000/user/root/output/test.txt";
        InputStream in = new URL(path).openStream();
        OutputStream ou = System.out;
        int buffer = 4096;
        boolean close = false;
        IOUtils.copyBytes(in, ou, buffer, close);
        IOUtils.closeStream(in);
    }

    @Test
    public void testCopyLocalFile() throws IOException, URISyntaxException{
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);

        Path src = new Path("input");
        Path dest = new Path("output");

        hdfs.copyFromLocalFile(src, dest);

        System.err.println("Upload to :" + conf.get("fs.default.name"));

        FileStatus files[] = hdfs.listStatus(dest);

        for(FileStatus file : files){
            System.err.println("结果：" + file.getPath());
        }
    }

    @Test
    public void testCopyLocalToRemoteFile() throws IOException, URISyntaxException, InterruptedException{
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(new URI("hdfs://192.168.105.9:9000/user/root/"), conf, "root");

        Path src = new Path("input");
        Path dest = new Path("output");

        hdfs.copyFromLocalFile(src, dest);

        System.err.println("Upload to :" + conf.get("fs.default.name"));

        FileStatus files[] = hdfs.listStatus(dest);

        for(FileStatus file : files){
            System.err.println("结果：" + file.getPath());
        }
    }

    @Test
    public void testGetData() throws IOException{
        Configuration conf = new Configuration();
//        conf.addResource("/usr/program_data/idea/elephant/conf/core-site.xml");
//        conf.addResource("/usr/program_data/idea/elephant/conf/hdfs-site.xml");

        System.err.println(conf.get("hadoop.tmp.dir"));
    }
}
