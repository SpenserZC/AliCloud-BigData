package com.aliyun.odps.lbs;

import java.util.Arrays;
import java.util.List;

import ch.hsr.geohash.GeoHash;

public class GeoHashUtil {
	/**
	 * 获取一点的GeoHash编码
	 * @param lat 维度
	 * @param lng 经度
	 * @param PRECISION GeoHash字符串长度
	 * @return
	 */
	public static String getGeoHash(final double lat,final double lng,int PRECISION){
		return GeoHash.geoHashStringWithCharacterPrecision(lat, lng, PRECISION);
	}
	
	public static List<String> getGeoHashFor9(final double lat,final double lng,int PRECISION){
		//字符串长度*5就是GeoHash的编码，除以2就是维度的切分的次数，也就是纬度的编码
		//如果编码长度为奇数，经度多切一次，否则相等
		int latLength= PRECISION*5/2;
		int lngLength= PRECISION*5%2==0?latLength:(latLength+1);
		//经度有360度，latLength为被切分的次数
		//维度有90度，lngLength为北切分的次数
		double minlat=180/Math.pow(2, latLength);
		double minlng=360/Math.pow(2, lngLength);
		
		//九宫格，对于每个点，都求出以它为中心的九宫格
		String leftTop = getGeoHash(lat+minlat,lng-minlng,PRECISION);
        String leftMid = getGeoHash(lat,lng-minlng,PRECISION);
        String leftBot = getGeoHash(lat-minlat,lng-minlng,PRECISION);
        String midTop = getGeoHash(lat+minlat,lng,PRECISION);
        String midMid = getGeoHash(lat,lng,PRECISION);
        String midBot = getGeoHash(lat-minlat,lng,PRECISION);
        String rightTop = getGeoHash(lat+minlat,lng+minlng,PRECISION);
        String rightMid = getGeoHash(lat,lng+minlng,PRECISION);
        String rightBot = getGeoHash(lat-minlat,lng+minlng,PRECISION);
		
        return Arrays.asList(leftTop,leftMid,leftBot,midTop,midMid,midBot,rightTop,rightMid,rightBot);

        
	}
	
	
}
