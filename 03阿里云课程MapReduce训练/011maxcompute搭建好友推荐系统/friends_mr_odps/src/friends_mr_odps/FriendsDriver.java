package friends_mr_odps;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;

public class FriendsDriver {

	public static void main(String[] args) throws OdpsException {
		JobConf job = new JobConf();

		// TODO: specify map output types
		job.setMapOutputKeySchema(SchemaUtils.fromString("friends:string"));
		job.setMapOutputValueSchema(SchemaUtils.fromString("count:bigint"));

		// TODO: specify input and output tables
		InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);
		OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(), job);
		
		job.setMapperClass(FriendsMapper.class);
		// TODO: specify a mapper
		job.setMapperClass(FriendsMapper.class);
		// TODO: specify a reducer
		job.setReducerClass(friendsReducer.class);

		RunningJob rj = JobClient.runJob(job);
		rj.waitForCompletion();
	}

}
