/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.sql;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import org.antlr.runtime.RecognitionException;
import org.apache.iotdb.db.sql.parse.AstNode;
import org.apache.iotdb.db.sql.parse.Node;
import org.apache.iotdb.db.sql.parse.ParseException;
import org.apache.iotdb.db.sql.parse.ParseUtils;
import org.junit.Test;

public class SQLParserTest {

  // Records Operations
  @Test
  public void createTimeseries() throws ParseException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_CREATE", "TOK_TIMESERIES", "TOK_PATH", "TOK_ROOT",
            "laptop", "d0", "s2", "TOK_WITH", "TOK_DATATYPE", "FLOAT", "TOK_ENCODING", "RLE",
            "TOK_CLAUSE",
            "freq_encoding", "DFT", "TOK_CLAUSE", "write_main_freq", "true", "TOK_CLAUSE",
            "dft_pack_length", "300",
            "TOK_CLAUSE", "dft_rate", "0.4", "TOK_CLAUSE", "write_encoding", "False"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator
        .generateAST("create timeseries root.laptop.d0.s2 with datatype=FLOAT"
            + ",encoding=RLE,freq_encoding=DFT,write_main_freq=true,dft_pack_length=300,dft_rate=0.4,write_encoding=False;");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void createTimeseries2() throws ParseException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_CREATE", "TOK_TIMESERIES", "TOK_PATH", "TOK_ROOT",
            "laptop", "d1", "s1", "TOK_WITH", "TOK_DATATYPE", "INT64", "TOK_ENCODING", "rle"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator
        .generateAST("create timeseries root.laptop.d1.s1 with datatype=INT64,encoding=rle");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void setStorageGroup1() throws ParseException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_SET", "TOK_STORAGEGROUP", "TOK_PATH", "TOK_ROOT", "a", "b", "c"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator.generateAST("set storage group to root.a.b.c");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void setStorageGroup2() throws ParseException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_SET", "TOK_STORAGEGROUP", "TOK_PATH", "TOK_ROOT", "a", "b", "c"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator.generateAST("set storage group to root.a.b.c;");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void multiInsert() throws ParseException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_INSERT", "TOK_PATH", "TOK_ROOT", "vehicle", "d0", "TOK_MULT_IDENTIFIER",
            "TOK_TIME",
            "s0", "s1", "s2", "TOK_MULT_VALUE", "12345678", "-1011.666", "'da#$%fa'", "FALSE",
            "True"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator.generateAST(
        "insert into root.vehicle.d0 (timestamp, s0, s1, s2)  values(12345678 , -1011.666, 'da#$%fa', FALSE, True)");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void multiInsert2() throws ParseException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_INSERT", "TOK_PATH", "TOK_ROOT", "vehicle", "d0", "TOK_MULT_IDENTIFIER",
            "TOK_TIME",
            "s0", "s1", "TOK_MULT_VALUE", "TOK_DATETIME", "now", "-1011.666", "1231"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator
        .generateAST(
            "insert into root.vehicle.d0 (timestamp, s0, s1)  values(now() , -1011.666, 1231);");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void multiInsert3() throws ParseException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_INSERT", "TOK_PATH", "TOK_ROOT", "vehicle", "d0", "TOK_MULT_IDENTIFIER",
            "TOK_TIME",
            "s0", "s1", "TOK_MULT_VALUE", "TOK_DATETIME", "2016-02-01 11:12:35", "-1011.666",
            "1231"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator.generateAST(
        "insert into root.vehicle.d0 (timestamp, s0, s1)  values(2016-02-01 11:12:35, -1011.666, 1231)");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void multiInsert4() throws ParseException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_INSERT", "TOK_PATH", "TOK_ROOT", "vehicle", "d0",
            "TOK_MULT_IDENTIFIER", "TOK_TIME", "s0", "s1", "TOK_MULT_VALUE", "TOK_DATETIME",
            "2016-02-01 11:12:35",
            "\'12\"3a\\'bc\'", "\"12\\\"3abc\""));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator.generateAST("insert into root.vehicle.d0 (timestamp, s0, s1) "
        + "values(2016-02-01 11:12:35, \'12\"3a\\'bc\', \"12\\\"3abc\")");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void updateValueWithTimeFilter1() throws ParseException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_UPDATE", "TOK_PATH", "TOK_ROOT", "laptop",
            "TOK_VALUE", "TOK_PATH", "d1", "s1", "-33000", "TOK_VALUE", "TOK_PATH", "mac", "d1",
            "s2", "'string'",
            "TOK_WHERE", "=", "TOK_PATH", "TOK_ROOT", "laptop", "d1", "s2", "TRUE"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator.generateAST(
        "UPDATE root.laptop SET d1.s1 = -33000, mac.d1.s2 = 'string' WHERE root.laptop.d1.s2 = TRUE");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void updateValueWithTimeFilter2() throws ParseException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_UPDATE", "TOK_PATH", "TOK_ROOT", "laptop", "*",
            "TOK_VALUE", "TOK_PATH", "d1", "s1", "-33000", "TOK_VALUE", "TOK_PATH", "mac", "d1",
            "s2", "'string'",
            "TOK_WHERE", "<", "TOK_PATH", "time", "100"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator
        .generateAST(
            "UPDATE root.laptop.* SET d1.s1 = -33000, mac.d1.s2 = 'string' WHERE time < 100;");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void updateValueWithTimeFilter3() throws ParseException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_UPDATE", "TOK_PATH", "TOK_ROOT", "laptop",
            "TOK_VALUE", "TOK_PATH", "d1", "s1", "-33.54", "TOK_VALUE", "TOK_PATH", "mac", "d1",
            "s2", "FALSE",
            "TOK_WHERE", "not", "=", "TOK_PATH", "d1", "s2", "TRUE"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator
        .generateAST(
            "UPDATE root.laptop SET d1.s1 = -33.54, mac.d1.s2 = FALSE WHERE not(d1.s2 = TRUE)");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void updateValueWithTimeFilter4() throws ParseException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_UPDATE", "TOK_PATH", "TOK_ROOT", "laptop",
            "TOK_VALUE", "TOK_PATH", "d1", "s1", "-33.54", "TOK_VALUE", "TOK_PATH", "mac", "d1",
            "s2", "FALSE",
            "TOK_WHERE", "and", "not", "<=", "TOK_PATH", "time", "1", "=", "TOK_PATH", "d1", "s2",
            "TRUE"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator.generateAST(
        "UPDATE root.laptop SET d1.s1 = -33.54, mac.d1.s2 = FALSE WHERE not(time <= 1) and  d1.s2 = TRUE;");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void delete() throws ParseException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_DELETE", "TOK_PATH", "TOK_ROOT", "d1", "s1",
            "TOK_WHERE", "<", "TOK_PATH", "time", "TOK_DATETIME", "2016-11-16 16:22:33+08:00"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator
        .generateAST("delete from root.d1.s1 where time < 2016-11-16 16:22:33+08:00");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void delete2() throws ParseException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_DELETE", "TOK_PATH", "TOK_ROOT", "d1", "s1",
            "TOK_WHERE", "<", "TOK_PATH", "time", "TOK_DATETIME", "now"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator.generateAST("delete from root.d1.s1 where time < now();");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void delete3() throws ParseException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_DELETE", "TOK_PATH", "TOK_ROOT", "d1", "s1",
            "TOK_WHERE", "<", "TOK_PATH", "time", "12345678909876"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator
        .generateAST("delete from root.d1.s1 where time < 12345678909876");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void delete4() throws ParseException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_DELETE", "TOK_PATH", "TOK_ROOT", "d1", "s1",
            "TOK_PATH", "TOK_ROOT", "d2", "s3", "TOK_WHERE", "<", "TOK_PATH", "time",
            "TOK_DATETIME", "now"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator
        .generateAST("delete from root.d1.s1,root.d2.s3 where time < now();");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void delete5() throws ParseException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_DELETE", "TOK_PATH", "TOK_ROOT", "d1", "*",
            "TOK_PATH", "TOK_ROOT", "*", "s2", "TOK_WHERE", "!", "<", "TOK_PATH", "time",
            "123456"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator
        .generateAST("delete from root.d1.*,root.*.s2 where !(time < 123456)");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  // @Test
  // public void loadData() throws ParseException{
  // // template for test case
  // ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_LOAD", "'/user/data/input.csv'", "root", "laptop",
  // "d1", "s1"));
  // ArrayList<String> rec = new ArrayList<>();
  // AstNode astTree = ParseGenerator.generateAST("LOAD TIMESERIES '/user/data/input.csv' root.laptop.d1.s1");
  // astTree = ParseUtils.findRootNonNullToken(astTree);
  // recursivePrintSon(astTree, rec);
  //
  // int i = 0;
  // while (i <= rec.size() - 1) {
  // assertEquals(rec.get(i), ans.get(i));
  // i++;
  // }
  // }

  @Test
  public void deleteTimeseires1() throws ParseException, RecognitionException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_DELETE", "TOK_TIMESERIES", "TOK_PATH", "TOK_ROOT", "dt", "a", "b", "d1",
            "s1"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator.generateAST("delete timeseries root.dt.a.b.d1.s1;");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void deleteTimeseires2() throws ParseException, RecognitionException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_DELETE", "TOK_TIMESERIES", "TOK_PATH", "TOK_ROOT", "*"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator.generateAST("delete timeseries root.*");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void deleteTimeseires3() throws ParseException, RecognitionException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_DELETE", "TOK_TIMESERIES", "TOK_PATH", "TOK_ROOT",
            "dt", "a", "b", "TOK_PATH", "TOK_ROOT", "*"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator.generateAST("delete timeseries root.dt.a.b,root.*;");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void query1() throws ParseException, RecognitionException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_QUERY", "TOK_SELECT", "TOK_PATH", "device_1",
            "sensor_1", "TOK_PATH", "device_2", "sensor_2", "TOK_FROM", "TOK_PATH", "TOK_ROOT",
            "vehicle",
            "TOK_WHERE", "and", "not", "<", "TOK_PATH", "TOK_ROOT", "laptop", "device_1",
            "sensor_1", "2000", ">",
            "TOK_PATH", "TOK_ROOT", "laptop", "device_2", "sensor_2", "1000"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator.generateAST(
        "SELECT device_1.sensor_1,device_2.sensor_2 FROM root.vehicle WHERE not(root.laptop.device_1.sensor_1 < 2000) and root.laptop.device_2.sensor_2 > 1000");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void query2() throws ParseException, RecognitionException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_QUERY", "TOK_SELECT", "TOK_PATH", "device_1",
            "sensor_1", "TOK_PATH", "device_2", "sensor_2", "TOK_FROM", "TOK_PATH", "TOK_ROOT",
            "vehicle",
            "TOK_WHERE", "&&", "<", "TOK_PATH", "TOK_ROOT", "laptop", "device_1", "sensor_1",
            "-2.2E10", ">",
            "TOK_PATH", "time", "TOK_DATETIME", "now"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator.generateAST(
        "SELECT device_1.sensor_1,device_2.sensor_2 FROM root.vehicle WHERE root.laptop.device_1.sensor_1 < -2.2E10 && time > now();");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void query3() throws ParseException, RecognitionException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_QUERY", "TOK_SELECT", "TOK_PATH", "device_1", "sensor_1", "TOK_PATH",
            "device_2",
            "sensor_2", "TOK_FROM", "TOK_PATH", "TOK_ROOT", "vehicle", "TOK_WHERE", "&", "<",
            "TOK_PATH",
            "time", "1234567", ">", "TOK_PATH", "time", "TOK_DATETIME", "2017-6-2T12:00:12+07:00"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator.generateAST(
        "SELECT device_1.sensor_1,device_2.sensor_2 FROM root.vehicle WHERE time < 1234567 & time > 2017-6-2T12:00:12+07:00");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void query4() throws ParseException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_QUERY", "TOK_SELECT", "TOK_PATH", "*", "TOK_FROM",
            "TOK_PATH", "TOK_ROOT", "vehicle", "TOK_WHERE", "||", "<", "TOK_PATH", "time",
            "1234567", ">",
            "TOK_PATH", "time", "TOK_DATETIME", "2017-6-2T12:00:12+07:00"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator
        .generateAST(
            "SELECT * FROM root.vehicle WHERE time < 1234567 || time > 2017-6-2T12:00:12+07:00;");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void aggregation1() throws ParseException, RecognitionException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_QUERY", "TOK_SELECT", "TOK_PATH", "TOK_CLUSTER",
            "TOK_PATH", "s1", "count", "TOK_PATH", "TOK_CLUSTER", "TOK_PATH", "s2", "max_time",
            "TOK_FROM",
            "TOK_PATH", "TOK_ROOT", "vehicle", "d1", "TOK_WHERE", "and", "<", "TOK_PATH",
            "TOK_ROOT", "vehicle",
            "d1", "s1", "0.32e6", "<=", "TOK_PATH", "time", "TOK_DATETIME", "now"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator.generateAST(
        "select count(s1),max_time(s2) from root.vehicle.d1 where root.vehicle.d1.s1 < 0.32e6 and time <= now()");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void aggregation2() throws ParseException, RecognitionException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_QUERY", "TOK_SELECT", "TOK_PATH", "TOK_CLUSTER",
            "TOK_PATH", "s2", "sum", "TOK_FROM", "TOK_PATH", "TOK_ROOT", "vehicle", "d1",
            "TOK_WHERE", "or", "<",
            "TOK_PATH", "s1", "2000", ">=", "TOK_PATH", "time", "1234567"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator
        .generateAST("select sum(s2) FROM root.vehicle.d1 WHERE s1 < 2000 or time >= 1234567;");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void aggregation3() throws ParseException, RecognitionException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_QUERY", "TOK_SELECT", "TOK_PATH", "s1", "TOK_PATH", "TOK_CLUSTER",
            "TOK_PATH", "s2",
            "sum", "TOK_FROM", "TOK_PATH", "TOK_ROOT", "vehicle", "d1", "TOK_WHERE", "|", "<",
            "TOK_PATH",
            "TOK_ROOT", "vehicle", "d1", "s1", "2000", ">=", "TOK_PATH", "time", "1234567"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator.generateAST(
        "select s1,sum(s2) FROM root.vehicle.d1 WHERE root.vehicle.d1.s1 < 2000 | time >= 1234567");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void groupby1() throws ParseException, RecognitionException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_QUERY", "TOK_SELECT", "TOK_PATH", "TOK_CLUSTER",
            "TOK_PATH", "s1", "count", "TOK_PATH", "TOK_CLUSTER", "TOK_PATH", "s2", "max_time",
            "TOK_FROM",
            "TOK_PATH", "TOK_ROOT", "vehicle", "d1", "TOK_WHERE", "and", "<", "TOK_PATH",
            "TOK_ROOT", "vehicle",
            "d1", "s1", "0.32e6", "<=", "TOK_PATH", "time", "TOK_DATETIME", "now", "TOK_GROUPBY",
            "TOK_TIMEUNIT",
            "10", "w", "TOK_TIMEORIGIN", "44", "TOK_TIMEINTERVAL", "TOK_TIMEINTERVALPAIR", "1", "3",
            "TOK_TIMEINTERVALPAIR", "4", "5"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator
        .generateAST("select count(s1),max_time(s2) " + "from root.vehicle.d1 "
            + "where root.vehicle.d1.s1 < 0.32e6 and time <= now() "
            + "group by(10w, 44, [1,3], [4,5])");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void groupby2() throws ParseException, RecognitionException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_QUERY", "TOK_SELECT", "TOK_PATH", "TOK_CLUSTER",
            "TOK_PATH", "s2", "sum", "TOK_FROM", "TOK_PATH", "TOK_ROOT", "vehicle", "d1",
            "TOK_WHERE", "or", "<",
            "TOK_PATH", "s1", "2000", ">=", "TOK_PATH", "time", "1234567", "TOK_GROUPBY",
            "TOK_TIMEUNIT", "111",
            "ms", "TOK_TIMEINTERVAL", "TOK_TIMEINTERVALPAIR", "123", "TOK_DATETIME",
            "2017-6-2T12:00:12+07:00",
            "TOK_TIMEINTERVALPAIR", "55555", "TOK_DATETIME", "now"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator
        .generateAST(
            "select sum(s2) " + "FROM root.vehicle.d1 " + "WHERE s1 < 2000 or time >= 1234567 "
                + "group by(111ms, [123,2017-6-2T12:00:12+07:00], [55555, now()]);");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void groupby3() throws ParseException, RecognitionException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_QUERY", "TOK_SELECT", "TOK_PATH", "s1", "TOK_PATH",
            "TOK_CLUSTER", "TOK_PATH", "s2", "sum", "TOK_FROM", "TOK_PATH", "TOK_ROOT", "vehicle",
            "d1",
            "TOK_WHERE", "|", "<", "TOK_PATH", "TOK_ROOT", "vehicle", "d1", "s1", "2000", ">=",
            "TOK_PATH", "time",
            "1234567", "TOK_GROUPBY", "TOK_TIMEUNIT", "111", "w", "TOK_TIMEORIGIN", "TOK_DATETIME",
            "2017-6-2T02:00:12+07:00", "TOK_TIMEINTERVAL", "TOK_TIMEINTERVALPAIR", "TOK_DATETIME",
            "2017-6-2T12:00:12+07:00", "TOK_DATETIME", "now"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator.generateAST("" + "select s1,sum(s2) " + "FROM root.vehicle.d1 "
        + "WHERE root.vehicle.d1.s1 < 2000 | time >= 1234567 "
        + "group by(111w, 2017-6-2T02:00:12+07:00, [2017-6-2T12:00:12+07:00, now()])");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void fill1() throws ParseException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_QUERY", "TOK_SELECT", "TOK_PATH", "s1", "TOK_FROM",
            "TOK_PATH", "TOK_ROOT", "vehicle", "d1", "TOK_WHERE", "=", "TOK_PATH", "time",
            "1234567", "TOK_FILL",
            "TOK_TYPE", "float", "TOK_PREVIOUS", "TOK_TIMEUNIT", "11", "h"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator.generateAST(
        "select s1 " + "FROM root.vehicle.d1 " + "WHERE time = 1234567 "
            + "fill(float[previous, 11h])");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void fill2() throws ParseException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_QUERY", "TOK_SELECT", "TOK_PATH", "s1", "TOK_FROM",
            "TOK_PATH", "TOK_ROOT", "vehicle", "d1", "TOK_WHERE", "=", "TOK_PATH", "time",
            "1234567", "TOK_FILL",
            "TOK_TYPE", "float", "TOK_PREVIOUS"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator.generateAST(
        "select s1 " + "FROM root.vehicle.d1 " + "WHERE time = 1234567 " + "fill(float[previous])");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void fill3() throws ParseException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_QUERY", "TOK_SELECT", "TOK_PATH", "s1", "TOK_FROM",
            "TOK_PATH", "TOK_ROOT", "vehicle", "d1", "TOK_WHERE", "=", "TOK_PATH", "time",
            "1234567", "TOK_FILL",
            "TOK_TYPE", "boolean", "TOK_LINEAR", "TOK_TIMEUNIT", "31", "s", "TOK_TIMEUNIT", "123",
            "d"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator.generateAST(
        "select s1 " + "FROM root.vehicle.d1 " + "WHERE time = 1234567 "
            + "fill(boolean[linear, 31s, 123d])");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void fill4() throws ParseException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_QUERY", "TOK_SELECT", "TOK_PATH", "s1", "TOK_FROM",
            "TOK_PATH", "TOK_ROOT", "vehicle", "d1", "TOK_WHERE", "=", "TOK_PATH", "time",
            "1234567", "TOK_FILL",
            "TOK_TYPE", "boolean", "TOK_LINEAR"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator.generateAST(
        "select s1 " + "FROM root.vehicle.d1 " + "WHERE time = 1234567 " + "fill(boolean[linear])");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void fill5() throws ParseException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_QUERY", "TOK_SELECT", "TOK_PATH", "s1", "TOK_PATH",
            "s2", "TOK_FROM", "TOK_PATH", "TOK_ROOT", "vehicle", "d1", "TOK_WHERE", "=", "TOK_PATH",
            "time",
            "1234567", "TOK_FILL", "TOK_TYPE", "int", "TOK_LINEAR", "TOK_TIMEUNIT", "5", "ms",
            "TOK_TIMEUNIT", "7",
            "d", "TOK_TYPE", "float", "TOK_PREVIOUS", "TOK_TYPE", "boolean", "TOK_LINEAR",
            "TOK_TYPE", "text",
            "TOK_PREVIOUS", "TOK_TIMEUNIT", "66", "w"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator
        .generateAST("select s1,s2 " + "FROM root.vehicle.d1 " + "WHERE time = 1234567 "
            + "fill(int[linear, 5ms, 7d], float[previous], boolean[linear], text[previous, 66w])");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  /*
   * Seven tests for LIMIT & SLIMIT.
   *
   * Test 1: when 'specialClause' = limitClause without offset. Test 2: when 'specialClause' = limitClause with
   * offset. Test 3: when 'specialClause' = slimitClause without soffset. Test 4: when 'specialClause' = slimitClause
   * with soffset. Test 5: when 'specialClause' contains both limitClause and slimitClause. Test 6: when
   * 'specialClause' contains groupbyClause, limitClause and slimitClause. Test 7: when 'specialClause' contains
   * fillClause and slimitClause.
   *
   */
  /*
   * Test 1: when 'specialClause' = limitClause without offset.
   *
   * Testing purpose: N in 'LIMIT N' should be NonNegativeInteger.
   *
   */
  @Test
  public void limit1() throws ParseException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_QUERY", "TOK_SELECT", "TOK_PATH", "device_1",
            "sensor_1", "TOK_PATH", "device_2", "sensor_2", "TOK_FROM", "TOK_PATH", "TOK_ROOT",
            "vehicle",
            "TOK_WHERE", "and", "not", "<", "TOK_PATH", "TOK_ROOT", "laptop", "device_1",
            "sensor_1", "2000", ">",
            "TOK_PATH", "TOK_ROOT", "laptop", "device_2", "sensor_2", "1000", "TOK_LIMIT", "0"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator.generateAST(
        "SELECT device_1.sensor_1,device_2.sensor_2 FROM root.vehicle WHERE not(root.laptop.device_1.sensor_1 < 2000) "
            + "and root.laptop.device_2.sensor_2 > 1000 LIMIT 0");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      // System.out.println(rec.get(i));
      i++;
    }
  }

  /*
   * Test 2: when 'specialClause' = limitClause with offset.
   *
   * Testing purposes: 1. N, OFFSETValue in 'LIMIT N OFFSET OFFSETValue' should both be NonNegativeInteger. 2.
   * offsetClause is invalid if without limitClause.
   *
   */
  @Test
  public void limit2() throws ParseException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_QUERY", "TOK_SELECT", "TOK_PATH", "device_1",
            "sensor_1", "TOK_PATH", "device_2", "sensor_2", "TOK_FROM", "TOK_PATH", "TOK_ROOT",
            "vehicle",
            "TOK_WHERE", "and", "not", "<", "TOK_PATH", "TOK_ROOT", "laptop", "device_1",
            "sensor_1", "2000", ">",
            "TOK_PATH", "TOK_ROOT", "laptop", "device_2", "sensor_2", "1000", "TOK_LIMIT", "10"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator.generateAST(
        "SELECT device_1.sensor_1,device_2.sensor_2 FROM root.vehicle WHERE not(root.laptop.device_1.sensor_1 < 2000) "
            + "and root.laptop.device_2.sensor_2 > 1000 LIMIT 10 OFFSET 2");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      // System.out.println(rec.get(i));
      i++;
    }
  }

  /*
   * Test 3: when 'specialClause' = slimitClause without soffset.
   *
   * Testing purposes: 1. SN in 'SLIMIT SN' should be NonNegativeInteger. 2. slimitClause is invalid if there is no
   * 'star' in the query paths.
   *
   */
  @Test
  public void limit3() throws ParseException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_QUERY", "TOK_SELECT", "TOK_PATH", "device_1",
            "sensor_1", "TOK_PATH", "device_2", "*", "TOK_FROM", "TOK_PATH", "TOK_ROOT", "vehicle",
            "TOK_WHERE",
            "&&", "<", "TOK_PATH", "TOK_ROOT", "laptop", "device_1", "sensor_1", "-2.2E10", ">",
            "TOK_PATH", "time",
            "TOK_DATETIME", "now", "TOK_SLIMIT", "10"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator
        .generateAST("SELECT device_1.sensor_1,device_2.* FROM root.vehicle "
            + "WHERE root.laptop.device_1.sensor_1 < -2.2E10 && time > now() SLIMIT 10");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      // System.out.println(rec.get(i));
      i++;
    }
  }

  /*
   * Test 4: when 'specialClause' = slimitClause with soffset.
   *
   * Testing purposes: 1. SN, SOFFSETValue in 'SLIMIT SN SOFFSET SOFFSETValue' should both be NonNegativeInteger. 2.
   * slimitClause is invalid if there is no 'star' in the query paths. 3. soffsetClause is invalid if without
   * slimitClause.
   *
   */
  @Test
  public void limit4() throws ParseException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_QUERY", "TOK_SELECT", "TOK_PATH", "device_1",
            "sensor_1", "TOK_PATH", "device_2", "sensor_2", "TOK_FROM", "TOK_PATH", "TOK_ROOT", "*",
            "TOK_WHERE",
            "&&", "<", "TOK_PATH", "TOK_ROOT", "laptop", "device_1", "sensor_1", "-2.2E10", ">",
            "TOK_PATH", "time",
            "TOK_DATETIME", "now", "TOK_SLIMIT", "10", "TOK_SOFFSET", "3"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator
        .generateAST("SELECT device_1.sensor_1,device_2.sensor_2 FROM root.* "
            + "WHERE root.laptop.device_1.sensor_1 < -2.2E10 && time > now() SLIMIT 10 SOFFSET 3");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      // System.out.println(rec.get(i));
      i++;
    }
  }

  /*
   * Test 5: when 'specialClause' contains both limitClause and slimitClause.
   *
   * Testing purposes: 1. NonNegativeInteger 2. slimitClause is invalid if there is no 'star' in the query paths. 3.
   * offsetClause is invalid if without limitClause. soffsetClause is invalid if without slimitClause. 4. The sequence
   * of limitClause and slimitClause should not affect the correctness of the parse.
   *
   */
  @Test
  public void limit5() throws ParseException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_QUERY", "TOK_SELECT", "TOK_PATH", "device_1", "*",
            "TOK_PATH", "device_2", "*", "TOK_FROM", "TOK_PATH", "TOK_ROOT", "vehicle", "TOK_WHERE",
            "&&", "<",
            "TOK_PATH", "TOK_ROOT", "laptop", "device_1", "sensor_1", "-2.2E10", ">", "TOK_PATH",
            "time",
            "TOK_DATETIME", "now", "TOK_LIMIT", "100", "TOK_SLIMIT", "10", "TOK_SOFFSET", "3"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator.generateAST("SELECT device_1.*,device_2.* FROM root.vehicle "
        + "WHERE root.laptop.device_1.sensor_1 < -2.2E10 && time > now() LIMIT 100 OFFSET 1 SLIMIT 10 SOFFSET 3");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      // System.out.println(rec.get(i));
      i++;
    }
  }

  /*
   * Test 6: when 'specialClause' contains groupbyClause, limitClause and slimitClause.
   *
   * Testing purpose: Test the correctness of the parse when groupbyClause, limitClause and slimitClause coexist.
   *
   */
  @Test
  public void limit6() throws ParseException, RecognitionException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_QUERY", "TOK_SELECT", "TOK_PATH", "TOK_CLUSTER",
            "TOK_PATH", "s1", "count", "TOK_PATH", "TOK_CLUSTER", "TOK_PATH", "s2", "max_time",
            "TOK_FROM",
            "TOK_PATH", "TOK_ROOT", "vehicle", "*", "TOK_WHERE", "and", "<", "TOK_PATH", "TOK_ROOT",
            "vehicle",
            "d1", "s1", "0.32e6", "<=", "TOK_PATH", "time", "TOK_DATETIME", "now", "TOK_GROUPBY",
            "TOK_TIMEUNIT",
            "10", "w", "TOK_TIMEORIGIN", "44", "TOK_TIMEINTERVAL", "TOK_TIMEINTERVALPAIR", "1", "3",
            "TOK_TIMEINTERVALPAIR", "4", "5", "TOK_SLIMIT", "1", "TOK_SOFFSET", "1", "TOK_LIMIT",
            "11"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator
        .generateAST("select count(s1),max_time(s2) " + "from root.vehicle.* "
            + "where root.vehicle.d1.s1 < 0.32e6 and time <= now() "
            + "group by(10w, 44, [1,3], [4,5])"
            + "slimit 1" + "soffset 1" + "limit 11" + "offset 3");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  /*
   * Test 7: when 'specialClause' contains fillClause and slimitClause.
   *
   * Testing purpose: Test the correctness of the parse when fillClause and slimitClause coexist.
   *
   */
  @Test
  public void limit7() throws ParseException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays
            .asList("TOK_QUERY", "TOK_SELECT", "TOK_PATH", "*", "TOK_FROM", "TOK_PATH", "TOK_ROOT",
                "vehicle",
                "d1", "TOK_WHERE", "=", "TOK_PATH", "time", "1234567", "TOK_FILL", "TOK_TYPE",
                "float",
                "TOK_PREVIOUS", "TOK_TIMEUNIT", "11", "h", "TOK_SLIMIT", "15", "TOK_SOFFSET",
                "50"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator
        .generateAST("select * " + "FROM root.vehicle.d1 " + "WHERE time = 1234567 "
            + "fill(float[previous, 11h])" + "slimit 15" + "soffset 50");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  // Authority Operation
  @Test
  public void createUser() throws ParseException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_CREATE", "TOK_USER", "myname", "TOK_PASSWORD", "mypwd"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator.generateAST("create user myname mypwd;");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void dropUser() throws ParseException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_DROP", "TOK_USER", "myname"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator.generateAST("drop user myname");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void updatePassword() throws ParseException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_UPDATE", "TOK_UPDATE_PSWD", "myname", "newpassword"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator.generateAST("update user myname set password newpassword;");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void createRole() throws ParseException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_CREATE", "TOK_ROLE", "admin"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator.generateAST("create role admin");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void dropRole() throws ParseException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_DROP", "TOK_ROLE", "admin"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator.generateAST("drop role admin;");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void grantUser() throws ParseException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_GRANT", "TOK_USER", "myusername", "TOK_PRIVILEGES",
            "'create'", "'delete'", "TOK_PATH", "TOK_ROOT", "laptop", "d1", "s1"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator
        .generateAST("grant user myusername privileges 'create','delete' on root.laptop.d1.s1");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void grantRole() throws ParseException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_GRANT", "TOK_ROLE", "admin", "TOK_PRIVILEGES",
            "'create'", "'delete'", "TOK_PATH", "TOK_ROOT", "laptop", "d1", "s1"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator
        .generateAST("grant role admin privileges 'create','delete' on root.laptop.d1.s1;");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void revokeUser() throws ParseException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_REVOKE", "TOK_USER", "myusername", "TOK_PRIVILEGES",
            "'create'", "'delete'", "TOK_PATH", "TOK_ROOT", "laptop", "d1", "s1"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator
        .generateAST("revoke user myusername privileges 'create','delete' on root.laptop.d1.s1");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void revokeRole() throws ParseException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_REVOKE", "TOK_ROLE", "admin", "TOK_PRIVILEGES",
            "'create'", "'delete'", "TOK_PATH", "TOK_ROOT", "laptop", "d1", "s1"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator
        .generateAST("revoke role admin privileges 'create','delete' on root.laptop.d1.s1;");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void grantRoleToUser() throws ParseException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_GRANT", "TOK_ROLE", "admin", "TOK_USER", "Tom"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator.generateAST("grant admin to Tom");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void revokeRoleFromUser() throws ParseException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_REVOKE", "TOK_ROLE", "admin", "TOK_USER", "Tom"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator.generateAST("revoke admin from Tom;");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void quit() throws ParseException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(Collections.singletonList("TOK_QUIT"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator.generateAST("quit");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  // Property Tree Operation
  @Test
  public void createProp() throws ParseException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_CREATE", "TOK_PROPERTY", "myprop"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator.generateAST("create property myprop");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void addLabelToProp() throws ParseException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_ADD", "TOK_LABEL", "myLabel", "TOK_PROPERTY", "myProp"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator.generateAST("ADD LABEL myLabel TO PROPERTY myProp;");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void delelteLabelFromProp() throws ParseException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_DELETE", "TOK_LABEL", "myLable", "TOK_PROPERTY", "myProp"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator.generateAST("DELETE LABEL myLable FROM PROPERTY myProp");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void linkLabel() throws ParseException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_LINK", "TOK_PATH", "TOK_ROOT", "a", "b", "c",
            "TOK_LABEL", "myLabel", "TOK_PROPERTY", "myProp"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator.generateAST("link root.a.b.c to myProp.myLabel;");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void unlinkLabel() throws ParseException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_UNLINK", "TOK_PATH", "TOK_ROOT", "m1", "m2",
            "TOK_LABEL", "myLabel", "TOK_PROPERTY", "myProp"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator.generateAST("unlink root.m1.m2 from myProp.myLabel");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  // Metadata
  @Test
  public void showMetadata() throws ParseException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_SHOW_METADATA"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator.generateAST("show metadata;");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  // others
  @Test
  public void describePath() throws ParseException, RecognitionException {
    // template for test case
    ArrayList<String> ans = new ArrayList<>(Arrays.asList("TOK_DESCRIBE", "TOK_PATH", "TOK_ROOT"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator.generateAST("describe root");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void createIndex1() throws ParseException, RecognitionException {
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_CREATE", "TOK_INDEX", "TOK_PATH", "TOK_ROOT", "a",
            "b", "c", "TOK_FUNC", "kvindex", "TOK_WITH", "TOK_INDEX_KV", "window_length", "50",
            "TOK_WHERE", ">",
            "TOK_PATH", "time", "123"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator
        .generateAST(
            "create index on root.a.b.c using kvindex with window_length=50 where time > 123");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void createIndex2() throws ParseException, RecognitionException {
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_CREATE", "TOK_INDEX", "TOK_PATH", "TOK_ROOT", "a",
            "b", "c", "TOK_FUNC", "kv-match2", "TOK_WITH", "TOK_INDEX_KV", "xxx", "50",
            "TOK_INDEX_KV", "xxx",
            "123", "TOK_WHERE", ">", "TOK_PATH", "time", "TOK_DATETIME", "now"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator
        .generateAST(
            "create index on root.a.b.c using kv-match2 with xxx=50,xxx=123 where time > now();");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void selectIndex1() throws ParseException, RecognitionException {
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_QUERY", "TOK_SELECT_INDEX", "subsequence_matching",
            "TOK_PATH", "TOK_ROOT", "a", "b", "c", "TOK_PATH", "TOK_ROOT", "a", "b", "c", "123",
            "132", "123.1",
            "TOK_WHERE", "<", "TOK_PATH", "time", "10"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator.generateAST(
        "select index subsequence_matching(root.a.b.c, root.a.b.c, 123, 132 , 123.1) where time < 10");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void selectIndex2() throws ParseException, RecognitionException {
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_QUERY", "TOK_SELECT_INDEX", "subsequence_matching",
            "TOK_PATH", "TOK_ROOT", "a", "b", "c", "TOK_PATH", "TOK_ROOT", "a", "b", "c", "123",
            "132", "123.1",
            "0.123", "0.5", "TOK_FROM", "TOK_PATH", "TOK_ROOT", "a", "b"));
    ArrayList<String> rec = new ArrayList<>();
    // AstNode astTree = ParseGenerator.generateAST("select index kvindex(root.vehicle.d0.s0, root.vehicle.d0.s0, 1,
    // 3, 0.0, 1.0, 0.0) from root.vehicle.d0.s0");
    AstNode astTree = ParseGenerator.generateAST(
        "select index subsequence_matching(root.a.b.c, root.a.b.c, 123, 132 , 123.1, 0.123, 0.5) from root.a.b;");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void selectIndex3() throws ParseException, RecognitionException {
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_QUERY", "TOK_SELECT_INDEX", "kvindex", "TOK_PATH",
            "TOK_ROOT", "a", "b", "c", "TOK_PATH", "TOK_ROOT", "a", "b", "c", "TOK_DATETIME",
            "2016-11-16T16:22:33+08:00", "TOK_DATETIME", "now", "123.1", "0.123", "0.5"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator.generateAST(
        "select index kvindex(root.a.b.c, root.a.b.c, 2016-11-16T16:22:33+08:00, now() , 123.1, 0.123, 0.5)");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  @Test
  public void dropIndex() throws ParseException, RecognitionException {
    ArrayList<String> ans = new ArrayList<>(
        Arrays.asList("TOK_DROP", "TOK_INDEX", "TOK_PATH", "TOK_ROOT", "a", "b", "c", "TOK_FUNC",
            "kvindex"));
    ArrayList<String> rec = new ArrayList<>();
    AstNode astTree = ParseGenerator.generateAST("drop index kvindex on root.a.b.c");
    astTree = ParseUtils.findRootNonNullToken(astTree);
    recursivePrintSon(astTree, rec);

    int i = 0;
    while (i <= rec.size() - 1) {
      assertEquals(rec.get(i), ans.get(i));
      i++;
    }
  }

  public void recursivePrintSon(Node ns, ArrayList<String> rec) {
    rec.add(ns.toString());
    if (ns.getChildren() != null) {
      for (Node nss : ns.getChildren()) {
        recursivePrintSon(nss, rec);
      }
    }
  }

}