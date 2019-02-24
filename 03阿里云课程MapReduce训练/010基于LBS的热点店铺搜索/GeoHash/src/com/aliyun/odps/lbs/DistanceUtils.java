package com.aliyun.odps.lbs;

public class DistanceUtils {
	/**
	 * 地球半径：6378.127km
	 */
	private static final double EARTH_RADIUS = 6378.393;
	/**
	 * 计算地球上两点距离
	 * @param lat1 第一个坐标维度
	 * @param lng1 第一个坐标经度
	 * @param lat2 第二个坐标纬度
	 * @param lng2 第二个坐标经度
	 * @return
	 */
	public static double getDistance(double lat1,double lng1,
			double lat2,double lng2){
		lat1 = rad(lat1);
	    lat2 = rad(lat2);
	    lng1 = rad(lng1);
	    lng2 = rad(lng2);
	    double s = EARTH_RADIUS*Math.acos(Math.cos(lng1-lng2)*Math.cos(lat1)*Math.cos(lat2)+Math.sin(lat1)*Math.sin(lat2));
	    return Math.round(s*1000);

	}
	
	public static double rad(double d){
		return d*Math.PI/180.0;
	}

}
