package com.ali;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;

public class TJDriver {

	public static void main(String[] args) throws OdpsException {
		JobConf job = new JobConf();

        // TODO: specify map output types

        job.setMapOutputKeySchema(SchemaUtils.fromString("pid:string"));

        job.setMapOutputValueSchema(SchemaUtils.fromString("pid:string,wid:bigint,totalq:bigint,avgq:double,nums:bigint,maxv:bigint"));

        // TODO: specify input and output tables

        InputUtils.addTable(TableInfo.builder().tableName(args[0]).partSpec("deviceid=001").build(),job);

        OutputUtils.addTable(TableInfo.builder().tableName(args[1]).partSpec("deviceid=001").build(),job);

        // TODO: specify a mapper

        job.setMapperClass(TJMapper.class);

        // TODO: specify a reducer

        job.setReducerClass(TJReducer.class);

        RunningJob rj = JobClient.runJob(job);

        rj.waitForCompletion();

	}

}

