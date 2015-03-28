package org.apache.hadoop.hbase.index.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;

public class CompressionTest {

  public static void main(String[] args) throws IOException{
    FileSystem fs=new Path("/root").getFileSystem(new Configuration());
    DefaultCodec codec=new GzipCodec();
    codec.setConf(new Configuration());
    
//    FSDataOutputStream output=fs.create(new Path("/root/test.gz"));
//    BufferedWriter writer=new BufferedWriter(new OutputStreamWriter(new GzipCodec().createOutputStream(output)));
//    writer.write("I am test!\n");
//    writer.write("Is it right?");
//    writer.close();
    
//    fs.setVerifyChecksum(false);
//    FSDataInputStream input=fs.open(new Path("/root/test.gz"));
//    BufferedReader reader=new BufferedReader(new InputStreamReader(codec.createInputStream(input)));
//    System.out.println(reader.readLine());
//    System.out.println(reader.readLine());
    
    String s="1111111111111111";
    byte[] b=Bytes.toBytes(s);
    for(int i=0;i<b.length;i++){
      String hex=Integer.toHexString(b[i]&0xff);
      if(hex.length()==1){
        hex='0'+hex;
      }
      System.out.print(hex);
    }
  }
}
