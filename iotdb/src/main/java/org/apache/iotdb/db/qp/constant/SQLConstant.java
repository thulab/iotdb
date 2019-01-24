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
package org.apache.iotdb.db.qp.constant;

import java.util.HashMap;
import java.util.Map;
import org.apache.iotdb.tsfile.read.common.Path;

/**
 * this class contains several constants used in SQL.
 */
public class SQLConstant {

  public static final String RESERVED_TIME = "time";
  public static final String RESERVED_FREQ = "freq";
  public static final String IS_AGGREGATION = "IS_AGGREGATION";
  public static final String NOW_FUNC = "now";
  public static final String START_TIME_STR = "1970-1-01T00:00:00";

  public static final String lineFeedSignal = "\n";
  public static final String ROOT = "root";
  public static final String METADATA_PARAM_EQUAL = "=";
  public static final String QUOTE = "'";
  public static final String DQUOTE = "\"";
  public static final String BOOLEN_TRUE = "true";
  public static final String BOOLEN_FALSE = "false";
  public static final String BOOLEAN_TRUE_NUM = "1";
  public static final String BOOLEAN_FALSE_NUM = "0";

  public static final int KW_AND = 1;
  public static final int KW_OR = 2;
  public static final int KW_NOT = 3;

  public static final int EQUAL = 11;
  public static final int NOTEQUAL = 12;
  public static final int LESSTHANOREQUALTO = 13;
  public static final int LESSTHAN = 14;
  public static final int GREATERTHANOREQUALTO = 15;
  public static final int GREATERTHAN = 16;
  public static final int EQUAL_NS = 17;

  public static final int TOK_SELECT = 21;
  public static final int TOK_FROM = 22;
  public static final int TOK_WHERE = 23;
  public static final int TOK_INSERT = 24;
  public static final int TOK_DELETE = 25;
  public static final int TOK_UPDATE = 26;
  public static final int TOK_QUERY = 27;

  public static final int TOK_CREATE_INDEX = 31;
  public static final int TOK_DROP_INDEX = 32;
  public static final int TOK_QUERY_INDEX = 33;

  public static final int TOK_AUTHOR_CREATE = 41;
  public static final int TOK_AUTHOR_DROP = 42;
  public static final int TOK_AUTHOR_GRANT = 43;
  public static final int TOK_AUTHOR_REVOKE = 44;
  public static final int TOK_AUTHOR_UPDATE_USER = 46;

  public static final int TOK_DATALOAD = 45;

  public static final int TOK_METADATA_CREATE = 51;
  public static final int TOK_METADATA_DELETE = 52;
  public static final int TOK_METADATA_SET_FILE_LEVEL = 53;
  public static final int TOK_PROPERTY_CREATE = 54;
  public static final int TOK_PROPERTY_ADD_LABEL = 55;
  public static final int TOK_PROPERTY_DELETE_LABEL = 56;
  public static final int TOK_PROPERTY_LINK = 57;
  public static final int TOK_PROPERTY_UNLINK = 58;
  public static final int TOK_LIST = 59;

  public static Map<Integer, String> tokenSymbol = new HashMap<>();
  public static Map<Integer, String> tokenNames = new HashMap<>();
  public static Map<Integer, Integer> reverseWords = new HashMap<>();

  static {
    tokenSymbol.put(KW_AND, "&");
    tokenSymbol.put(KW_OR, "|");
    tokenSymbol.put(KW_NOT, "!");
    tokenSymbol.put(EQUAL, "=");
    tokenSymbol.put(NOTEQUAL, "<>");
    tokenSymbol.put(EQUAL_NS, "<=>");
    tokenSymbol.put(LESSTHANOREQUALTO, "<=");
    tokenSymbol.put(LESSTHAN, "<");
    tokenSymbol.put(GREATERTHANOREQUALTO, ">=");
    tokenSymbol.put(GREATERTHAN, ">");
  }

  static {
    tokenNames.put(KW_AND, "and");
    tokenNames.put(KW_OR, "or");
    tokenNames.put(KW_NOT, "not");
    tokenNames.put(EQUAL, "equal");
    tokenNames.put(NOTEQUAL, "not_equal");
    tokenNames.put(EQUAL_NS, "equal_ns");
    tokenNames.put(LESSTHANOREQUALTO, "lessthan_or_equalto");
    tokenNames.put(LESSTHAN, "lessthan");
    tokenNames.put(GREATERTHANOREQUALTO, "greaterthan_or_equalto");
    tokenNames.put(GREATERTHAN, "greaterthan");

    tokenNames.put(TOK_SELECT, "TOK_SELECT");
    tokenNames.put(TOK_FROM, "TOK_FROM");
    tokenNames.put(TOK_WHERE, "TOK_WHERE");
    tokenNames.put(TOK_INSERT, "TOK_INSERT");
    tokenNames.put(TOK_DELETE, "TOK_DELETE");
    tokenNames.put(TOK_UPDATE, "TOK_UPDATE");
    tokenNames.put(TOK_QUERY, "TOK_QUERY");

    tokenNames.put(TOK_AUTHOR_CREATE, "TOK_AUTHOR_CREATE");
    tokenNames.put(TOK_AUTHOR_DROP, "TOK_AUTHOR_DROP");
    tokenNames.put(TOK_AUTHOR_GRANT, "TOK_AUTHOR_GRANT");
    tokenNames.put(TOK_AUTHOR_REVOKE, "TOK_AUTHOR_REVOKE");
    tokenNames.put(TOK_AUTHOR_UPDATE_USER, "TOK_AUTHOR_UPDATE_USER");
    tokenNames.put(TOK_DATALOAD, "TOK_DATALOAD");

    tokenNames.put(TOK_METADATA_CREATE, "TOK_METADATA_CREATE");
    tokenNames.put(TOK_METADATA_DELETE, "TOK_METADATA_DELETE");
    tokenNames.put(TOK_METADATA_SET_FILE_LEVEL, "TOK_METADATA_SET_FILE_LEVEL");
    tokenNames.put(TOK_PROPERTY_CREATE, "TOK_PROPERTY_CREATE");
    tokenNames.put(TOK_PROPERTY_ADD_LABEL, "TOK_PROPERTY_ADD_LABEL");
    tokenNames.put(TOK_PROPERTY_DELETE_LABEL, "TOK_PROPERTY_DELETE_LABEL");
    tokenNames.put(TOK_PROPERTY_LINK, "TOK_PROPERTY_LINK");
    tokenNames.put(TOK_PROPERTY_UNLINK, "TOK_PROPERTY_UNLINK");

    tokenNames.put(TOK_LIST, "TOK_LIST");
  }

  static {
    reverseWords.put(KW_AND, KW_OR);
    reverseWords.put(KW_OR, KW_AND);
    reverseWords.put(EQUAL, NOTEQUAL);
    reverseWords.put(NOTEQUAL, EQUAL);
    reverseWords.put(LESSTHAN, GREATERTHANOREQUALTO);
    reverseWords.put(GREATERTHANOREQUALTO, LESSTHAN);
    reverseWords.put(LESSTHANOREQUALTO, GREATERTHAN);
    reverseWords.put(GREATERTHAN, LESSTHANOREQUALTO);
  }

  public static boolean isReservedPath(Path pathStr) {
    return pathStr.equals(SQLConstant.RESERVED_TIME) || pathStr.equals(SQLConstant.RESERVED_FREQ);

  }
}
