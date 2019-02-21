package com.aliyun.odps.lbs;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;

public class LBSDriver {

	public static void main(String[] args) throws OdpsException {
		JobConf job = new JobConf();
		job.set("TABLE_SRC", args[0]);
        job.set("TABLE_POI", args[1]);
        job.set("TABLE_OUT", args[2]);
        
        job.setMapperClass(LBSMapper.class);
		job.setReducerClass(LBSReducer.class);
		
		job.setMapOutputKeySchema(SchemaUtils.fromString("word:string,type:bigint"));
		job.setMapOutputValueSchema(SchemaUtils.fromString("id:string,lat:double,lng:double"));

		//以下设置分区的列
        String cols[] = {"hash"};
        job.setPartitionColumns(cols);
        //设置输出的table名称
        InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);
        InputUtils.addTable(TableInfo.builder().tableName(args[1]).build(), job);
        //设置输出的table名称
        OutputUtils.addTable(TableInfo.builder().tableName(args[2]).build(), job);
		
		RunningJob rj = JobClient.runJob(job);
		rj.waitForCompletion();
	}

}
