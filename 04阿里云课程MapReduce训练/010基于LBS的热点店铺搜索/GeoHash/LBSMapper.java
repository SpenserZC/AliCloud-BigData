package com.aliyun.odps.lbs;

import java.io.IOException;
import java.util.List;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;

public class LBSMapper extends MapperBase {
	private static final int PRECISION = 5;
 
    
	private Record value;
    private static String TABLE_SRC = null;
    private static String TABLE_POI = null;

	@Override
	public void setup(TaskContext context) throws IOException {
		value = context.createMapOutputValueRecord();
        TABLE_SRC = context.getJobConf().get("TABLE_SRC");
        TABLE_POI = context.getJobConf().get("TABLE_POI");

	}

	@Override
	public void map(long recordNum, Record record, TaskContext context) throws IOException {
		 //获取记录中的经度和纬度值，为表中的第二列和第三列
        Double lng = record.getDouble(1);
        Double lat = record.getDouble(2);
        long type = 0;
        //根据表的来源设置类型字段
        if (TABLE_SRC.equalsIgnoreCase(context.getInputTableInfo().getTableName())) {
            type = 1;
        } else if (TABLE_POI.equalsIgnoreCase(context.getInputTableInfo().getTableName())) {
            type = 2;
        }

        value.set(0, record.get(0)); //第一个字段是id
        value.setDouble(1, lat);
        value.setDouble(2, lng);
        if (type == 1L) {
          //类型为1表示该数据为用户地点数据，需要计算9个区域的geoHash编码
            List<String> hashes = GeoHashUtil.getGeoHashFor9(lat, lng, PRECISION);
            for (String hash : hashes) {
                Record key = context.createMapOutputKeyRecord();
                key.set(0, hash);
                key.set(1, type);
                //9个输出的数据  除了key的第一个字段geoHash编码不一样，其他的全都一样
                context.write(key, value);
            }
        } else {
            //如果是店铺数据，则只需计算店铺地点的geoHash编码并输出即可
            Record key = context.createMapOutputKeyRecord();
            String hash = GeoHashUtil.getGeoHash(lat, lng, PRECISION);
            key.set(0, hash);
            key.set(1, type);
            context.write(key, value);
        }
    }


	@Override
	public void cleanup(TaskContext context) throws IOException {
	}

}
