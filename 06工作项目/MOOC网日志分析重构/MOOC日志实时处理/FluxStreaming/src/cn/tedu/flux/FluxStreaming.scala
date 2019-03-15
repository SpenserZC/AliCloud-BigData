package cn.tedu.flux

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Base64
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.filter.Filter
import org.apache.hadoop.hbase.filter.KeyOnlyFilter
import java.util.Calendar
import org.apache.hadoop.hbase.filter.RowFilter
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.RegexStringComparator
import scala.util.Random
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.client.Put
import java.sql.DriverManager
import java.sql.Date
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet

object FluxStreaming {
  var sc: SparkContext = null;
  def main(args: Array[String]): Unit = {
    //1.创建sc
    val conf = new SparkConf()
    conf.setAppName("Flux_Streaming")
    conf.setMaster("local[5]")
    sc = new SparkContext(conf)

    saveToHBase("flux", "rk", "cf1", "c1", "v1111");
    //2.创建ssc
    val ssc = new StreamingContext(sc, Seconds(3))

    //3.从kafka中消费数据
    val zk = "hadoop01:2181,hadoop02:2181,hadoop03:2181"
    val gid = "gx01"
    val topics = Map("fluxtopic" -> 1)
    val stream = KafkaUtils.createStream(ssc, zk, gid, topics)

    //4.清洗数据
    val clearDStream = stream.map(_._2)
      .map(_.split("\\|"))
      .map(arr => {
        Map("url" -> arr(0), "urlname" -> arr(1), "ref" -> arr(11), "uagent" -> arr(12), "uvid" -> arr(13), "ssid" -> arr(14).split("_")(0), "sscount" -> arr(14).split("_")(1), "sstime" -> arr(14).split("_")(2), "cip" -> arr(15))
      });

    //5.数据存储到hbase中
    clearDStream.foreachRDD(_.foreach(clearMap => {
      val tab = "flux"
      var rk = clearMap("sstime") + "_" + clearMap("uvid") + "_" + clearMap("ssid") + "_" + clearMap("cip") + "_";
      rk = rk + RandomUtils.getRandInt(64 - rk.length())
      clearMap.foreach(t => {
        val k = t._1
        val v = t._2
        saveToHBase(tab, rk, "cf1", k, v);
      })
    }));

    //6.数据处理
    //Map(sstime -> 1552446453176, uvid -> 94429308642124234912, url -> http://localhost/FluxAppServer/b.jsp, urlname -> b.jsp, ssid -> 3503044545, ref -> http://localhost/FluxAppServer/a.jsp, cip -> 0:0:0:0:0:0:0:1, sscount -> 23, uagent -> Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36)
    //(pv,uv,vv,br,newip,newcust,avgtime,avgdeep)
    val resultDStream = clearDStream.map(clearMap => {
      //pv - 访问量 - 一条日志就是一个pv
      val pv = 1;

      //uv - 独立访客数 - 一天之内访客的数量 - 一天之内所有uvid去重后计数 
      //- 如果当前的uvid在今天这条数据之前没有出现过则uv=1 否则uv=0
      //- 需要保存所有已经处理过的数据 - 中间数据的存储 - 海量数据 实时增删改查 灵活访问 - Hbase
      //- Hbase要创建表 create 'flux','cf1'
      val sstime = clearMap("sstime").toLong;
      val calendar = Calendar.getInstance();
      calendar.setTimeInMillis(sstime);
      calendar.set(Calendar.HOUR_OF_DAY, 0)
      calendar.set(Calendar.MINUTE, 0)
      calendar.set(Calendar.SECOND, 0)
      calendar.set(Calendar.MILLISECOND, 0)
      val zerotime = calendar.getTimeInMillis()
      val uvid = clearMap("uvid")
      val filter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator("^\\d+_" + uvid + "_.*$"))
      val qr1 = queryFromHBase("flux", (zerotime + "").getBytes(), (sstime + "").getBytes(), filter)
      val uv = if (qr1.count() == 0) 1 else 0;

      //--会话总数 - 一天之内会话的总的数量 - 得到当前会话编号ssid 查看今天这条数据之前是否出现过这个ssid
      //中间输出存储 hbase
      val ssid = clearMap("ssid")
      val filter2 = new RowFilter(CompareOp.EQUAL, new RegexStringComparator("^\\d+_\\d+_" + ssid + "_.*$"))
      val qr2 = queryFromHBase("flux", (zerotime + "").getBytes, (sstime + "").getBytes, filter2)
      val vv = if (qr2.count() == 0) 1 else 0;

      //--跳出率 - 一天之内跳出的会话总数占总的会话数的比率 - 无法计算
      //val br = 0;

      //--新增ip总数 - 一天之内ip地址去重后在历史数据中从未出现过的数量 - 用当前cip 到历史数据中寻找 看是否出现过
      val cip = clearMap("cip")
      val filter3 = new RowFilter(CompareOp.EQUAL, new RegexStringComparator("^\\d+_\\d+_\\d+_" + cip + "_.*$"))
      val qr3 = queryFromHBase("flux", null, (sstime + "").getBytes, filter3)
      val newip = if (qr3.count() == 0) 1 else 0;

      //--新增客户总数 - 一天之内uvid去重后在历史数据中从未出现过的数量 - 用当前uvid 到历史数据中寻找 看是否出现过
      val filter4 = new RowFilter(CompareOp.EQUAL, new RegexStringComparator("^\\d+_" + uvid + "_.*$"))
      val qr4 = queryFromHBase("flux", null, (sstime + "").getBytes, filter4)
      val newcust = if (qr4.count() == 0) 1 else 0; //uvid sstime

      //--平均访问时长 - 一天之内所有会话访问时长的平均值 - 无法计算
      //val avgtime = 0;

      //--平均访问深度 - 一天之内所有会话访问深度的平均 - 无法计算
      //val avgdeep = 0;

      (sstime, pv, uv, vv, newip, newcust)
    })

    //将结果写入mysql
    //use fluxdb;
    //create table tongji2(reportTime date,pv int,uv int,vv int,newip int,newcust int);
    resultDStream.foreachRDD(_.foreach(t => {
      Class.forName("com.mysql.jdbc.Driver");
      var conn: Connection = null;
      var ps: PreparedStatement = null;
      var rs: ResultSet = null;
      try {
        conn = DriverManager.getConnection("jdbc:mysql:///fluxdb", "root", "root")
        ps = conn.prepareStatement("select * from tongji2 where reportTime = ?")
        ps.setDate(1, new Date(t._1));
        rs = ps.executeQuery()
        if (rs.next()) {
          val ps = conn.prepareStatement("update tongji2 set pv=pv+?,uv=uv+?,vv=vv+?,newip=newip+?,newcust=newcust+? where reportTime = ? ")
          ps.setInt(1, t._2);
          ps.setInt(2, t._3);
          ps.setInt(3, t._4);
          ps.setInt(4, t._5);
          ps.setInt(5, t._6);
          ps.setDate(6, new Date(t._1))
          ps.executeUpdate()
        } else {
          val ps = conn.prepareStatement("insert into tongji2 values (?,?,?,?,?,?)")
          ps.setDate(1, new Date(t._1))
          ps.setInt(2, t._2);
          ps.setInt(3, t._3);
          ps.setInt(4, t._4);
          ps.setInt(5, t._5);
          ps.setInt(6, t._6);
          ps.executeUpdate()
        }
      } catch {
        case t: Throwable => t.printStackTrace()
      } finally {
        if (rs != null) {
          try {
            rs.close();
          } catch {
            case t: Throwable => {
              rs = null
              t.printStackTrace()
            }
          }
        }
        if (ps != null) {
          try {
            ps.close();
          } catch {
            case t: Throwable => {
              ps = null
              t.printStackTrace()
            }
          }
        }
        if (conn != null) {
          try {
            conn.close();
          } catch {
            case t: Throwable => {
              conn = null
              t.printStackTrace()
            }
          }
        }
      }
    }))

    resultDStream.print()

    //8.启动streaming
    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * 从hbase中查询数据
   */
  def queryFromHBase(tname: String, startRow: Array[Byte], stopRow: Array[Byte], filter: Filter) = {
    //1.创建sc
    //2.指定Hbase配置对象，指定hbase基本配置信息
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "hadoop01,hadoop02,hadoop03")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set(TableInputFormat.INPUT_TABLE, tname)

    //3.设置过滤条件 
    val scan = new Scan();
    if (startRow != null) {
      scan.setStartRow(startRow)
    }
    if (stopRow != null) {
      scan.setStopRow(stopRow)
    }
    if (filter != null) {
      scan.setFilter(filter)
    }
    conf.set(TableInputFormat.SCAN, Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray))

    //3.读取hbase
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    //4.遍历数据
    val r = hBaseRDD.map {
      case (_, result) => {
        var map: Map[String, String] = Map();
        //获取行键  
        val key = Bytes.toString(result.getRow)
        map = map + ("key" -> key)
        //通过列族和列名获取列
        val it = result.listCells().iterator();
        while (it.hasNext()) {
          val c = it.next();
          val q = new String(c.getQualifier())
          val v = new String(c.getValue())
          map = map + (q -> v)
        }
        map
      }
    }
    r
  }
  /**
   * 向hbase中写入数据
   */
  def saveToHBase(tab: String, rk: String, cf: String, c: String, v: String) {
    //2.创建hbase配置对象,指定基本配置
    val hbaseConf = HBaseConfiguration.create();
    hbaseConf.set("hbase.zookeeper.quorum", "hadoop01,hadoop02,hadoop03")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")

    //3.创建JobConf,指定输出的表名
    val jobConf = new JobConf(hbaseConf);
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tab)

    //4.准备数据
    val rdd = sc.makeRDD(Array(Array(rk, cf, c, v)));
    val toHBaseRDD = rdd.map(arr => {
      val put = new Put(arr(0).getBytes);
      put.add(arr(1).getBytes, arr(2).getBytes, arr(3).getBytes)
      (new ImmutableBytesWritable, put)
    })
    //5.向HBases写入数据
    toHBaseRDD.saveAsHadoopDataset(jobConf);
  }
}