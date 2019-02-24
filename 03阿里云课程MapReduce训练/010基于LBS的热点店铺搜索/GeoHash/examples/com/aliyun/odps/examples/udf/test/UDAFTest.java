package com.aliyun.odps.examples.udf.test;

import java.io.IOException;
import java.util.List;

import com.aliyun.odps.udf.local.LocalRunException;
import com.aliyun.odps.udf.local.datasource.InputSource;
import com.aliyun.odps.udf.local.datasource.TableInputSource;
import com.aliyun.odps.udf.local.runner.AggregatorRunner;
import com.aliyun.odps.udf.local.runner.BaseRunner;

/**
 * 
 * you can also write an UT
 * 
 */
public class UDAFTest {

  public static void main(String[] args) throws LocalRunException, IOException {
    // //////////////test1: simple input/////////////////
    BaseRunner runner = new AggregatorRunner(null,
        "com.aliyun.odps.examples.udf.UDAFExample");
    runner.feed(new Object[] { "one", "one" }).feed(new Object[] { "three", "three" })
        .feed(new Object[] { "four", "four" });
    List<Object[]> out = runner.yield();
    TestUtil.assertEquals(1 + "", out.size() + "");
    TestUtil.assertEquals(24 + "", out.get(0)[0] + "");

    runner = new AggregatorRunner(TestUtil.getOdps(),
        "com.aliyun.odps.examples.udf.UDAFExample");
    // partition table
    String project = "example_project";
    String table = "wc_in2";
    String[] partitions = new String[] { "p2=1", "p1=2" };
    String[] columns = new String[] { "colc", "cola" };

    // //////////////test2: input from table/////////////////
    InputSource inputSource = new TableInputSource(project, table, partitions, columns);
    Object[] data;
    while ((data = inputSource.getNextRow()) != null) {
      runner.feed(data);
    }
    out = runner.yield();
    TestUtil.assertEquals(1 + "", out.size() + "");
    TestUtil.assertEquals(36 + "", out.get(0)[0] + "");

    // //////////////test3: resource test/////////////////
    runner = new AggregatorRunner(TestUtil.getOdps(),
        "com.aliyun.odps.examples.udf.UDAFResource");
    runner.feed(new Object[] { "one", "one" }).feed(new Object[] { "three", "three" })
        .feed(new Object[] { "four", "four" });
    out = runner.yield();
    TestUtil.assertEquals(1 + "", out.size() + "");
    // 24+3+4+4
    TestUtil.assertEquals(35 + "", out.get(0)[0] + "");

    System.out.println("Pass");
  }

}
