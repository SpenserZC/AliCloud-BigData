package com.aliyun.odps.lbs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;

public class LBSReducer extends ReducerBase {
	private static final double DISTANCE = 1000d;
	
	private Record result = null;
    //用来保存用户地点
    Map<String, LocationBean> business = null;
    private String lastHash = null;

	@Override
	public void setup(TaskContext context) throws IOException {
		 
		 result = context.createOutputRecord();
         business = new HashMap();	
	}
	 
	String tempKey = "";
	@Override
	public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
		tempKey = key.getString(0);
        while (values.hasNext()) {
            Record val = values.next();
            String hash = key.getString(0);

            if (key.getBigint(1).intValue() == 1) {
                //如果这次处理的hash值和上次的不一样，则清空用户地点数据
                if (null == lastHash || !lastHash.equals(hash)) {
                    business.clear();
                    lastHash = hash;
                }

                business.put(val.getString(0), new LocationBean(val.getDouble(1), val.getDouble(2)));
            } else {
                //将店铺地点与所有的用户地址计算距离并比较，输出小于1公里的数据
                for (String k : business.keySet()) {
                    LocationBean p = business.get(k);
                    double d = DistanceUtils.getDistance(p.getLat(), p.getLng(), val.getDouble(1),
                            val.getDouble(2));
                    if (d < DISTANCE) {
                        result.set(0, k);
                        result.set(1, val.getString(0));
                        result.set(2, d);
                        context.write(result);
                    }
                }
            }
        }
	}

	@Override
	public void cleanup(TaskContext context) throws IOException {
	}

}
