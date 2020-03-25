package com.robin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import sun.nio.ch.IOUtil;

import java.io.IOException;
import java.io.OutputStream;

public class SampleHdfs {

    public static void createDir(String dst) {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.155.101:9000");

        try {
            FileSystem fs = FileSystem.get(conf);
            fs.mkdirs(new Path(dst));
            System.out.println("create file successfully!");
            fs.close();
        } catch (IOException e) {
            System.out.println("create file error!");
            e.getStackTrace();
        }
    }

    public static void pushFile(String dst, String aim) {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.155.101:9000");

        try {
            FileSystem fs = FileSystem.get(conf);
            fs.copyFromLocalFile(new Path(dst), new Path(aim));
            System.out.println("push file successfully!");
            fs.close();
        } catch (IOException e) {
            System.out.println("push file error!");
            e.getStackTrace();
        }
    }

    public static void delFile(String dst) {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.155.101:9000");

        try {
            FileSystem fs = FileSystem.get(conf);
            fs.delete(new Path(dst),true); // TRUE represents recursive deletion/
            System.out.println("delete file successfully!");
            fs.close();
        } catch (IOException e) {
            System.out.println("delete file error!");
            e.getStackTrace();
        }
    }

    public static void readFile(String dst) {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.155.101:9000");
        FSDataInputStream in = null;

        try {
            FileSystem fs = FileSystem.get(conf);
            in = fs.open(new Path(dst));
            OutputStream out = System.out;
            IOUtils.copyBytes(in, out, 4096, true);
            fs.close();
        } catch (IOException e) {
            System.out.println("read file error!");
            e.getStackTrace();
        } finally {
            IOUtils.closeStream(in);
        }
    }

    public static void main(String[] args) throws IOException {
        //createDir("/testCreateFile");
        //pushFile("/home/robin/Desktop/cv", "/file/input");
        //delFile("/file/input");
        readFile("/file/input");
    }
}
