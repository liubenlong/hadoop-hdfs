package com.lbl.hadoop.mr.recommend;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 前两个参数分别是mapper传过来的key和value。
 * 后两个参数书最终输出的结果
 * 
 * @author liubenlong3
 *
 */
public class FriendsReCommendReduce extends Reducer<Text, Text, Text, Text>{
	
	@Override
	protected void reduce(Text text, Iterable<Text> iterable, Context context)
			throws IOException, InterruptedException {
		
		Set<String> set = new HashSet<String>();
		for (Text text2 : iterable) {
			set.add(text2.toString());
		}
		
		System.out.println(set);
		
		if(set.size() > 1) {
			//下面这个是计算笛卡尔积，这里通过mapper计算以后，每一个set里的数据就是有共同好友的。所以他们各自都是2度好友关系。
			for (Iterator<String> iterator = set.iterator(); iterator.hasNext();) {
				String name =  iterator.next();
				
				for (Iterator<String> iterator2 = set.iterator(); iterator2.hasNext();) {
					String other =  iterator2.next();
					
					if(!name.equals(other)){
						context.write(new Text(name), new Text(other));
					}
				}
			}
		}
		
	}
}
