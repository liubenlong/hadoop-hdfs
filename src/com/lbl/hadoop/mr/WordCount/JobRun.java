package com.lbl.hadoop.mr.WordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobRun {

	//这里与视频教程里讲的稍有区别，主要是如何执行。我这里没有出问题，一切正常。ok 
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://192.168.196.129:9000");
	
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobRun.class);
			job.setMapperClass(WordCountMapper.class);
			job.setReducerClass(WordCountReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			
//		job.setNumReduceTasks(1);//设置reduce任务的数量。默认值就是1
			
			FileInputFormat.addInputPath(job, new Path("wordCount/wordCountIn.txt"));
			FileOutputFormat.setOutputPath(job, new Path("wordCount/wordCountOut.txt"));//如果已经有了该文件，可能会执行出错。要先删掉
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
