package com.ali;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;

public class TJReducer extends ReducerBase {
	
	private Record result;

    private long alln=0;//总放电次数  

    private long allq=0;//总放电量                

    private List<R> recs;

	
	@Override
	public void setup(TaskContext context) throws IOException {
		 result=context.createOutputRecord();

         recs=new ArrayList<R>();
	
	}

	@Override
	public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
		float a_n=0;//次数均值          
        float a_q=0;//放电量均值                        
        float f_n=0;//次数方差           
        float f_q=0;//放电量方差                                        
        while (values.hasNext()) {
                 Record temp=values.next();
                 R r=new R();
                 r.nums=temp.getBigint("nums");
                 r.avgq=temp.getDouble("avgq");
                 r.totalq=temp.getBigint("totalq");
                 r.max=temp.getBigint("maxv");
                 r.wid=temp.getBigint("wid");                   
                 recs.add(r);
                 alln=alln+temp.getBigint("nums");
                 allq=allq+temp.getBigint("totalq");                 
        }
        //均值计算
        for(int i=0;i<recs.size();i++)
        {
                 recs.get(i).n_p=(float)recs.get(i).nums/alln;
                 recs.get(i).q_p=(float)recs.get(i).totalq/allq;
                 a_n=a_n+recs.get(i).n_p*recs.get(i).wid;
                 a_q=a_q+recs.get(i).q_p*recs.get(i).wid;                        
        }     
        //方差计算
        for(int i=0;i<recs.size();i++)
        {
                 float a=recs.get(i).n_p*(float)Math.pow(recs.get(i).wid-a_n, 2);                   
                 f_n=f_n+a;
                 float b=recs.get(i).q_p*(float)Math.pow(recs.get(i).wid-a_q, 2); 
                 f_q=f_q+b;                                   
        }
        f_n=(float)Math.sqrt((double)f_n);
        f_q=(float)Math.sqrt((double)f_q);         
        //sk计算公式的分子
        float ln=0;
        float lq=0;     
       //计算sk
        for(int i=0;i<recs.size();i++)
        {
                 float a=recs.get(i).n_p*(float)Math.pow(recs.get(i).wid-a_n, 3);                   
                 ln=ln+a;
                 float b=recs.get(i).q_p*(float)Math.pow(recs.get(i).wid-a_q, 3); 
                 lq=lq+b;                               
        }
        double sk_n=ln/Math.pow(f_n, 3);
        double sk_q=lq/Math.pow(f_q, 3);
        //添加结果集
        result.set("pid", key.getString(0));
        result.set("skn", sk_n);
        result.set("skq", sk_q);
        //重置临时累计变量
        ln=0;
        lq=0;
        //计算正半轴ku
        for(int i=0;i<recs.size();i++)
        {
                 float a=recs.get(i).n_p*(float)Math.pow(recs.get(i).wid-a_n, 4);                   
                 ln=ln+a;
                 float b=recs.get(i).q_p*(float)Math.pow(recs.get(i).wid-a_q, 4); 
                 lq=lq+b;                               
        }

        double ku_n=ln/Math.pow(f_n, 4)-3;
        double ku_q=lq/Math.pow(f_q, 4)-3;      

       

        //添加结果集          
        result.set("kun", ku_n);
        result.set("kuq", ku_q);           
        context.write(result);    
	}

	@Override
	public void cleanup(TaskContext context) throws IOException {
	}

}
