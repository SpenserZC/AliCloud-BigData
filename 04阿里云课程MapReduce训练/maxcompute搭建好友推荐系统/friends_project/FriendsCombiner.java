package com.aliyun.friends;

import java.io.IOException;
import java.util.Iterator;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;

public class FriendsCombiner extends ReducerBase {
	private Record result;
	
	@Override
	public void setup(TaskContext context) throws IOException {
		result =context.createMapOutputValueRecord();
	
	}

	@Override
	public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
		long count=0;
		//计算key中的两个人有多少个共同好友
		while (values.hasNext()) {
			Record val=values.next();
			if(0==(Long)val.get(0)){
				count = 0;
				break;
			}
			count +=(Long)val.get(0);
		}
		result.set(new Object[]{count});
		context.write(key,result);
	}

	@Override
	public void cleanup(TaskContext context) throws IOException {
	}

}
