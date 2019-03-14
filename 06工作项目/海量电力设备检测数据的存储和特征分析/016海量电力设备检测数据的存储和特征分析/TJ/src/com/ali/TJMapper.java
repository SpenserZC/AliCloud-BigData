package com.ali;

import java.io.IOException;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;

public class TJMapper extends MapperBase {
	
	private Record val;

    private Record k;
	
	@Override
	public void setup(TaskContext context) throws IOException {
		 k=context.createMapOutputKeyRecord();

         val=context.createMapOutputValueRecord();

	}

	@Override
	public void map(long recordNum, Record record, TaskContext context) throws IOException {
		String pid= record.getString("pid");                 

        k.set(new Object[] {pid});                                    

        val=record;                                  

        context.write(k,val);

	}

	@Override
	public void cleanup(TaskContext context) throws IOException {
	}

}
