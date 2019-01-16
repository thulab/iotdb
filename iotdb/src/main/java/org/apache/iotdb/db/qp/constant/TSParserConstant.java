/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.qp.constant;

import java.util.HashMap;
import java.util.Map;
import org.apache.iotdb.db.sql.parse.TSParser;

public class TSParserConstant {
    private static Map<Integer, Integer> antlrQpMap = new HashMap<>();

    // used to get operator type when construct operator from AST Tree
    static {
        antlrQpMap.put(TSParser.KW_AND, SqlConstant.KW_AND);
        antlrQpMap.put(TSParser.KW_OR, SqlConstant.KW_OR);
        antlrQpMap.put(TSParser.KW_NOT, SqlConstant.KW_NOT);

        antlrQpMap.put(TSParser.EQUAL, SqlConstant.EQUAL);
        antlrQpMap.put(TSParser.NOTEQUAL, SqlConstant.NOTEQUAL);
        antlrQpMap.put(TSParser.LESSTHANOREQUALTO, SqlConstant.LESSTHANOREQUALTO);
        antlrQpMap.put(TSParser.LESSTHAN, SqlConstant.LESSTHAN);
        antlrQpMap.put(TSParser.LESSTHANOREQUALTO, SqlConstant.LESSTHANOREQUALTO);
        antlrQpMap.put(TSParser.LESSTHAN, SqlConstant.LESSTHAN);
        antlrQpMap.put(TSParser.GREATERTHANOREQUALTO, SqlConstant.GREATERTHANOREQUALTO);
        antlrQpMap.put(TSParser.GREATERTHAN, SqlConstant.GREATERTHAN);
        antlrQpMap.put(TSParser.EQUAL_NS, SqlConstant.EQUAL_NS);

        antlrQpMap.put(TSParser.TOK_SELECT, SqlConstant.TOK_SELECT);
        antlrQpMap.put(TSParser.TOK_FROM, SqlConstant.TOK_FROM);
        antlrQpMap.put(TSParser.TOK_WHERE, SqlConstant.TOK_WHERE);
        antlrQpMap.put(TSParser.TOK_QUERY, SqlConstant.TOK_QUERY);
    }

    public static int getTSTokenIntType(int antlrIntType) {
        if (!antlrQpMap.containsKey(antlrIntType)) {
            System.out.println(antlrIntType);
        }
        return antlrQpMap.get(antlrIntType);
    }

}
