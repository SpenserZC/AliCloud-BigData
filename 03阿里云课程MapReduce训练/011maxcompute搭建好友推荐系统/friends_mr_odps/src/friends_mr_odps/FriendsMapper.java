package friends_mr_odps;

import java.io.IOException;
import java.util.Arrays;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;

public class FriendsMapper extends MapperBase {
	private Record key;
	private Record value;
	
	@Override
	public void setup(TaskContext context) throws IOException {
		key = context.createMapOutputKeyRecord();
		value=context.createMapOutputValueRecord();
		System.out.println("TaskID"+context.getTaskID().toString());
	}

	@Override
	public void map(long recordNum, Record record, TaskContext context) throws IOException {
		//读取第一列数据写入变量user
		String user=record.get(0).toString();
		//将输入表中第二列数据和第一列数据写入变量all，并且第一列数据和第二列数据
		//使用" "分割
		String all=user+" "+record.get(1).toString();
		//将输入表中每条数据拆分，拆分键为“空格”，拆分成String类型的数组arr
		String[] arr = all.split(" ");
		Arrays.sort(arr);
		int len = arr.length;
		for (int i=0; i<len-1; ++i) {
            for(int j=i+1; j<len; ++j) {
              key.set(new Object[] {arr[i] + " " + arr[j]}); 
              if(arr[i].equals(user)||arr[j].equals(user)) {
                value.set(new Object[] {0L});
                context.write(key, value);
              }
              else {          
                value.set(new Object[] {1L});
                context.write(key,value); 
              }    
            }
         }
	}	

	@Override
	public void cleanup(TaskContext context) throws IOException {
	}

}
