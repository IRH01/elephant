package com.irh.elephant.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;

/**
 * Created by iritchie on 17-1-11.
 */
public class HDFSTest{

    //initialization
//    private Configuration conf = new Configuration();
//    private FileSystem hdfs;

//    @BeforeTest
//    public void setUp() throws IOException{
//        String path = "/usr/program_data/idea/elephant/conf";
//        conf.addResource(new Path(path + "core-site.xml"));
//        conf.addResource(new Path(path + "hdfs-siete.xml"));
//        conf.addResource(new Path(path + "mapred-site.xml"));
////        path = "/usr/java/hbase-0.90.3/conf/";
////        conf.addResource(new Path(path + "hbase-site.xml"));
//        hdfs = FileSystem.get(conf);
//    }

    @Test
    public void testOperator() throws IOException{
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
        String path = "hdfs://192.168.105.9:9000/user/root/input/log4j.properties";
        InputStream in = new URL(path).openStream();
        OutputStream ou = System.out;
        int buffer = 4096;
        boolean close = false;
        IOUtils.copyBytes(in, ou, buffer, close);

        IOUtils.closeStream(in);
    }

//    //create a direction
//    @Test
//    public void createDir() throws IOException{
//        String dir = "/test";
//        Path path = new Path(dir);
//        hdfs.mkdirs(path);
//        System.out.println("new dir \t" + conf.get("fs.default.name") + dir);
//    }
//
//    //copy from local file to HDFS file
//    @Test
//    public void copyFile() throws IOException{
//        String hdfsDst = "/test";
//        String localSrc = "/home/ictclas/Configure.xml";
//
//        Path src = new Path(localSrc);
//        Path dst = new Path(hdfsDst);
//        hdfs.copyFromLocalFile(src, dst);
//
//        //list all the files in the current direction
//        FileStatus files[] = hdfs.listStatus(dst);
//        System.out.println("Upload to \t" + conf.get("fs.default.name") + hdfsDst);
//        for(FileStatus file : files){
//            System.out.println(file.getPath());
//        }
//    }
//
//    //create a new file
//    @Test
//    public void createFile() throws IOException{
//        String fileName = "/test/word.txt";
//        String fileContent = "Hello, world! Just a test.";
//
//        Path dst = new Path(fileName);
//        byte[] bytes = fileContent.getBytes();
//        FSDataOutputStream output = hdfs.create(dst);
//        output.write(bytes);
//        System.out.println("new file \t" + conf.get("fs.default.name") + fileName);
//    }
//
//    //list all files
//    public void listFiles(String dirName) throws IOException{
//
//        Path f = new Path(dirName);
//        FileStatus[] status = hdfs.listStatus(f);
//        System.out.println(dirName + " has all files:");
//        for(int i = 0; i < status.length; i++){
//            System.out.println(status[i].getPath().toString());
//        }
//    }
//
//    //judge a file existed? and delete it!
//    public void deleteFile(String fileName) throws IOException{
//        Path f = new Path(fileName);
//        boolean isExists = hdfs.exists(f);
//        if(isExists){    //if exists, delete
//            boolean isDel = hdfs.delete(f, true);
//            System.out.println(fileName + "  delete? \t" + isDel);
//        }else{
//            System.out.println(fileName + "  exist? \t" + isExists);
//        }
//    }

}
