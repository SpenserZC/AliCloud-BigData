package cn.tedu.web;

import java.io.IOException;
import java.net.URLDecoder;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

public class LogServlet extends HttpServlet {

	private static Logger logger = Logger.getLogger(LogServlet.class);
	
	public void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		String qs = request.getQueryString();
		URLDecoder decoder = new URLDecoder();
		qs = decoder.decode(qs,"utf-8");
		String [] kvs = qs.split("&");
		String log = "";
		for(String kv : kvs){
			String v = kv.split("=").length<2 ? "" : kv.split("=")[1];
			log += v+"|";
		}
		String ip = request.getRemoteAddr();
		log += ip;
		
		//--通过log4j记录日志 - 将日志发送给flume
		logger.info(log);
	}

	public void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		doGet(request,response);
	}

}
