package com.lbl.hadoop.hdfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * HDFS测试类
 * @author liubenlong3
 *
 */
public class HelloWord {
	
	Configuration conf = new Configuration();
	FileSystem hdfs = null;
	
	@Before
	public void init(){
		try {
			conf.set("fs.default.name", "hdfs://192.168.196.129:9000");
			hdfs = FileSystem.get(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@After
	public void closeHDFS(){
		try {
			if(null != hdfs) hdfs.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 上传文件
	 * @
	 */
	@Test
	public void uploadFile(){
		FSDataOutputStream outputStream = null;
		try {
			Path path = new Path("in/helloWord.txt");
			outputStream = hdfs.create(path);
			byte[] buffer = " 你好Hello".getBytes();
			outputStream.write(buffer, 0, buffer.length);
			outputStream.flush();
			System.out.println("Create OK");
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if(null != outputStream) outputStream.close();
			} catch (IOException e) {
				e.printStackTrace();
			}			
		}
	}
	
	 /**
	  * 在HDFS上删除一个文件
	  * 
	  * **/
	 @Test
	 public void deleteFileOnHDFS(){
	    try {
			Path p =new Path("friendsRecommend/qqOut.txt");
			hdfs.deleteOnExit(p);
			System.out.println("删除成功.....");
		} catch (IOException e) {
			e.printStackTrace();
		}
	 }
	/**
	 * 将本地文件拷贝到hdfs系统中
	 * @
	 */
	@Test
	public void uploadFileFromLocal() {
		try {
			hdfs.copyFromLocalFile(new Path("D://qq.txt"), new Path("friendsRecommend/qqIn.txt"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("Create OK");
	}
	
	/**
	 * 重名名一个文件夹或者文件
	 * @
	 */
	@Test
	 public void renameFileOrDirectoryOnHDFS(){
	    try {
			Path p1 =new Path("in/android工具类 ftpsftp上传.txt");
			Path p2 =new Path("in/android工具类 ftpsftp上传11.txt");
			hdfs.rename(p1, p2);
			System.out.println("重命名文件夹或文件成功.....");
		} catch (IOException e) {
			e.printStackTrace();
		}
	 }
	
	
	/***
	  *  
	  * 读取HDFS某个文件夹的所有
	  * 文件，并打印文件路径和内容
	  * 
	  * **/
	@Test
    public void readHDFSListAll() {
		try {
			//获取日志文件的根目录
			Path listf = new Path("in/");
			//获取根目录下的所有2级子文件目录
			FileStatus stats[] = hdfs.listStatus(listf);
			for(int i = 0; i < stats.length; i++){
				//获取子目录下的文件路径
				FileStatus temp[] = hdfs.listStatus(new Path(stats[i].getPath().toString()));
				for(int k = 0; k < temp.length;k++){
					//打开文件流
					InputStream in = null;
					//BufferedReader包装一个流
					BufferedReader buff = null;
					try {
						System.out.println("文件路径名:"+temp[k].getPath().toString());
						//获取Path
						Path p=new Path(temp[k].getPath().toString());
						in = hdfs.open(p);
						buff = new BufferedReader(new InputStreamReader(in));	       	 
						String str = null;
						while((str=buff.readLine())!=null){
							System.out.println(str);
						}
					} catch (Exception e) {
						e.printStackTrace();
					} finally{
						if(null != buff) buff.close();
						if(null != in) in.close();
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	  * 从HDFS上下载文件或文件夹到本地
	  * 
	  * **/
	@Test
	 public void downloadFileorDirectoryOnHDFS() {
	    try {
			Path p1 =new Path("in/helloWord.txt");
			Path p2 =new Path("D://7.txt");
			hdfs.copyToLocalFile(p1, p2);
			System.out.println("下载文件夹或文件成功.....");
		} catch (IOException e) {
			e.printStackTrace();
		}
		 
	 }
	 
	
	 /**
	  * 在HDFS上创建一个文件夹
	  * 
	  * **/
	@Test
	 public void createDirectoryOnHDFS(){
	    try {
			Path p =new Path("a/b/c");
			hdfs.mkdirs(p);
			System.out.println("创建文件夹成功.....");
		} catch (IOException e) {
			e.printStackTrace();
		}
		 
	 }
	 
	 /**
	  * 在HDFS上删除一个文件夹
	  * 
	  * **/
	@Test
	 public void deleteDirectoryOnHDFS(){
	    try {
			Path p =new Path("a/b/c");
			hdfs.deleteOnExit(p);
			System.out.println("删除文件夹成功.....");
		} catch (IOException e) {
			e.printStackTrace();
		}
		 
	 }
	 /**
	  * 在HDFS上创建一个文件
	  * 
	  * **/
	 @Test
	 public void createFileOnHDFS(){
	    try {
			Path p =new Path("a/b/test.txt");
			hdfs.createNewFile(p);
			System.out.println("创建文件成功.....");
		} catch (IOException e) {
			e.printStackTrace();
		}
		 
	 }
	 
	
}
