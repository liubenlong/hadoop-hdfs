package com.lbl.hadoop.mr.recommend;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Map-Reduce测试类--统计2度联系人
 * @author liubenlong3
 *
 *	Mapper四个泛型的含义：
 *	第一个：入参的key;
 *	第二个：入参的value；
 *	第3个：出参的key；
 *	第4个：出参的value；
 *
 *	由于这里是统计单词的数量，所以，入参的值是单词的下标，value是单词；出参的key是单词，value是出现的数量。这都是根据具体业务来定的类型。
 */
public class FriendsReCommendMapper extends Mapper<LongWritable, Text, Text, Text>{

	/**
	 * 第一个参数：入参 的key【改行数据在文件中的下标】;
	 * 第2个参数：入参的value
	 * 
	 * 每次调用该方法会传入split中一行数据的key和value
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();//一行数据
		
		String[] split = line.split(" ");
		
		context.write(new Text(split[0]), new Text(split[1]));
		context.write(new Text(split[1]), new Text(split[0]));
	
	}

}
