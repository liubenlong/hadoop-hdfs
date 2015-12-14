package com.lbl.hadoop.mr.WordCount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 前两个参数分别是mapper传过来的key和value。
 * 后两个参数书最终输出的结果
 * 很明显，输入都是word【String类型】，value都是数量【int类型】
 * 
 * @author liubenlong3
 *
 */
public class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
	/**
	 * 第二个参数之所以是个迭代器，是因为这个集合可能会灰常灰常的大
	 */
	@Override
	protected void reduce(Text key, Iterable<IntWritable> iterable, Context context)
			throws IOException, InterruptedException {
		
		int sum = 0;
		for (IntWritable intWritable : iterable) {
			sum = sum + intWritable.get();
		}
		
		context.write(key, new IntWritable(sum));
	}
}
