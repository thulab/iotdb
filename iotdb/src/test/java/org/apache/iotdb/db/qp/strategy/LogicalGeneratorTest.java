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
package org.apache.iotdb.db.qp.strategy;

import static org.junit.Assert.assertEquals;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import org.apache.iotdb.db.exception.qp.LogicalOperatorException;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class LogicalGeneratorTest {

  LogicalGenerator generator;

  @Before
  public void setUp() throws Exception {
    generator = new LogicalGenerator(ZonedDateTime.now().getOffset());
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testParseTimeFormatNow() throws LogicalOperatorException {
    long now = generator.parseTimeFormat(SQLConstant.NOW_FUNC);
    for (int i = 0; i <= 12; i++) {
      ZoneOffset offset1, offset2;
      if (i < 10) {
        offset1 = ZoneOffset.of("+0" + i + ":00");
        offset2 = ZoneOffset.of("-0" + i + ":00");
      } else {
        offset1 = ZoneOffset.of("+" + i + ":00");
        offset2 = ZoneOffset.of("-" + i + ":00");
      }
      ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(now),
          ZoneId.of(offset1.toString()));
      assertEquals(now, zonedDateTime.toInstant().toEpochMilli());
      zonedDateTime = ZonedDateTime
          .ofInstant(Instant.ofEpochMilli(now), ZoneId.of(offset2.toString()));
      assertEquals(now, zonedDateTime.toInstant().toEpochMilli());
    }

  }

  @Test(expected = LogicalOperatorException.class)
  public void testParseTimeFormatFail1() throws LogicalOperatorException {
    generator.parseTimeFormat(null);
  }

  @Test(expected = LogicalOperatorException.class)
  public void testParseTimeFormatFail2() throws LogicalOperatorException {
    generator.parseTimeFormat("");
  }
}
