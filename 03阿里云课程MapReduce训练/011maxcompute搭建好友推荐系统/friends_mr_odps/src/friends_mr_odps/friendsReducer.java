package friends_mr_odps;

import java.io.IOException;
import java.util.Iterator;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.mapred.ReducerBase;

public class friendsReducer extends ReducerBase {
	
	private LongWritable sum=new LongWritable();
	private Text user1 = new Text();
	private Text user2 = new Text();
	private Record result=null;
	
	@Override
	public void setup(TaskContext context) throws IOException {
	
		result=context.createOutputRecord();
	}

	@Override
	public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {	
		
		int count = 0;
		while (values.hasNext()) {
		  Record val = values.next();
		  if( 0 == (Long)val.get(0) ) {
		     count = 0;
		     break;
		   }
		   count += (Long)val.get(0);
		}
		
//		Record val = values.next();
//		Long count =(Long)values.next().get(0);
		if(count > 0) {
		    sum.set(count);
		    String user=key.get(0).toString();
		    String[] users = user.split(" ");
		    user1.set(users[0]);
		    user2.set(users[1]);
		    result.set(0, user1);
		    result.set(1, user2);
		    result.set(2, sum);
		    context.write(result);
		  }
	}

	@Override
	public void cleanup(TaskContext context) throws IOException {
	}

}
