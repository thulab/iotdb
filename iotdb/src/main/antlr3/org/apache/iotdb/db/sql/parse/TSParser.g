//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

parser grammar TSParser;

options
{
tokenVocab=TSLexer;
output=AST;
ASTLabelType=CommonTree;
backtrack=false;
k=3;
}

tokens {

//update
TOK_SHOW_METADATA;
TOK_MERGE;
TOK_QUIT;
TOK_PRIVILEGES;
TOK_USER;
TOK_INDEX;
TOK_ROLE;
TOK_CREATE;
TOK_DROP;
TOK_GRANT;
TOK_REVOKE;
TOK_UPDATE;
TOK_VALUE;
TOK_INSERT;
TOK_QUERY;
TOK_SELECT;
TOK_GROUPBY;
TOK_FILL;
TOK_TYPE;
TOK_LINEAR;
TOK_PREVIOUS;
TOK_TIMEUNIT;
TOK_TIMEORIGIN;
TOK_TIMEINTERVAL;
TOK_TIMEINTERVALPAIR;
TOK_PASSWORD;
TOK_PATH;
TOK_UPDATE_PSWD;
TOK_FROM;
TOK_WHERE;
TOK_CLUSTER;
TOK_LOAD;
TOK_METADATA;
TOK_NULL;
TOK_ISNULL;
TOK_ISNOTNULL;
TOK_DATETIME;
TOK_DELETE;
TOK_INDEX_KV;
TOK_FUNC;
TOK_SELECT_INDEX;
TOK_LIST;
TOK_ALL;
TOK_SLIMIT;
TOK_SOFFSET;
TOK_LIMIT;

/*
  BELOW IS THE METADATA TOKEN
*/
TOK_MULT_VALUE;
TOK_MULT_IDENTIFIER;
TOK_TIME;
TOK_WITH;
TOK_ROOT;
TOK_DATATYPE;
TOK_ENCODING;
TOK_CLAUSE;
TOK_TIMESERIES;
TOK_SET;
TOK_ADD;
TOK_PROPERTY;
TOK_LABEL;
TOK_LINK;
TOK_UNLINK;
TOK_STORAGEGROUP;
TOK_DESCRIBE;
}


@header {
package org.apache.iotdb.db.sql.parse;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;

}


@members{
ArrayList<ParseError> errors = new ArrayList<ParseError>();
    Stack msgs = new Stack<String>();

    private static HashMap<String, String> xlateMap;
    static {
        //this is used to support auto completion in CLI
        xlateMap = new HashMap<String, String>();

        // Keywords
        // xlateMap.put("KW_TRUE", "TRUE");
        // xlateMap.put("KW_FALSE", "FALSE");

        xlateMap.put("KW_AND", "AND");
        xlateMap.put("KW_OR", "OR");
        xlateMap.put("KW_NOT", "NOT");
        xlateMap.put("KW_LIKE", "LIKE");

        xlateMap.put("KW_BY", "BY");
        xlateMap.put("KW_GROUP", "GROUP");
        xlateMap.put("KW_FILL", "FILL");
        xlateMap.put("KW_LINEAR", "LINEAR");
        xlateMap.put("KW_PREVIOUS", "PREVIOUS");
        xlateMap.put("KW_WHERE", "WHERE");
        xlateMap.put("KW_FROM", "FROM");

        xlateMap.put("KW_SELECT", "SELECT");
        xlateMap.put("KW_INSERT", "INSERT");

        xlateMap.put("KW_LIMIT","LIMIT");
        xlateMap.put("KW_OFFSET","OFFSET");
        xlateMap.put("KW_SLIMIT","SLIMIT");
        xlateMap.put("KW_SOFFSET","SOFFSET");

        xlateMap.put("KW_ON", "ON");
        xlateMap.put("KW_ROOT", "ROOT");

        xlateMap.put("KW_SHOW", "SHOW");

        xlateMap.put("KW_CLUSTER", "CLUSTER");

        xlateMap.put("KW_LOAD", "LOAD");

        xlateMap.put("KW_NULL", "NULL");
        xlateMap.put("KW_CREATE", "CREATE");

        xlateMap.put("KW_DESCRIBE", "DESCRIBE");

        xlateMap.put("KW_TO", "TO");
        xlateMap.put("KW_ON", "ON");
        xlateMap.put("KW_USING", "USING");

        xlateMap.put("KW_DATETIME", "DATETIME");
        xlateMap.put("KW_TIMESTAMP", "TIMESTAMP");
        xlateMap.put("KW_TIME", "TIME");
        xlateMap.put("KW_CLUSTERED", "CLUSTERED");

        xlateMap.put("KW_INTO", "INTO");

        xlateMap.put("KW_ROW", "ROW");
        xlateMap.put("KW_STORED", "STORED");
        xlateMap.put("KW_OF", "OF");
        xlateMap.put("KW_ADD", "ADD");
        xlateMap.put("KW_FUNCTION", "FUNCTION");
        xlateMap.put("KW_WITH", "WITH");
        xlateMap.put("KW_SET", "SET");
        xlateMap.put("KW_UPDATE", "UPDATE");
        xlateMap.put("KW_VALUES", "VALUES");
        xlateMap.put("KW_KEY", "KEY");
        xlateMap.put("KW_ENABLE", "ENABLE");
        xlateMap.put("KW_DISABLE", "DISABLE");
        xlateMap.put("KW_ALL", "ALL");

        // Operators
        xlateMap.put("DOT", ".");
        xlateMap.put("COLON", ":");
        xlateMap.put("COMMA", ",");
        xlateMap.put("SEMICOLON", ");");

        xlateMap.put("LPAREN", "(");
        xlateMap.put("RPAREN", ")");
        xlateMap.put("LSQUARE", "[");
        xlateMap.put("RSQUARE", "]");

        xlateMap.put("EQUAL", "=");
        xlateMap.put("NOTEQUAL", "<>");
        xlateMap.put("EQUAL_NS", "<=>");
        xlateMap.put("LESSTHANOREQUALTO", "<=");
        xlateMap.put("LESSTHAN", "<");
        xlateMap.put("GREATERTHANOREQUALTO", ">=");
        xlateMap.put("GREATERTHAN", ">");

        xlateMap.put("CharSetLiteral", "\\'");
        xlateMap.put("KW_LIST", "LIST");
    }

    public static Collection<String> getKeywords() {
        return xlateMap.values();
    }

    private static String xlate(String name) {

        String ret = xlateMap.get(name);
        if (ret == null) {
            ret = name;
        }

        return ret;
    }

    @Override
    public Object recoverFromMismatchedSet(IntStream input,
                                           RecognitionException re, BitSet follow) throws RecognitionException {
        throw re;
    }

    @Override
    public void displayRecognitionError(String[] tokenNames,
                                        RecognitionException e) {
        errors.add(new ParseError(this, e, tokenNames));
    }

    @Override
    public String getErrorHeader(RecognitionException e) {
        String header = null;
        if (e.charPositionInLine < 0 && input.LT(-1) != null) {
            Token t = input.LT(-1);
            header = "line " + t.getLine() + ":" + t.getCharPositionInLine();
        } else {
            header = super.getErrorHeader(e);
        }

        return header;
    }

    @Override
    public String getErrorMessage(RecognitionException e, String[] tokenNames) {
        String msg = null;

        // Translate the token names to something that the user can understand
        String[] xlateNames = new String[tokenNames.length];
        for (int i = 0; i < tokenNames.length; ++i) {
            xlateNames[i] = TSParser.xlate(tokenNames[i]);
        }

        if (e instanceof NoViableAltException) {
            @SuppressWarnings("unused")
            NoViableAltException nvae = (NoViableAltException) e;
            // for development, can add
            // "decision=<<"+nvae.grammarDecisionDescription+">>"
            // and "(decision="+nvae.decisionNumber+") and
            // "state "+nvae.stateNumber
            msg = "cannot recognize input near "
                + input.LT(1) != null ? " " + getTokenErrorDisplay(input.LT(1)) : ""
                + input.LT(1) != null ? " " + getTokenErrorDisplay(input.LT(1)) : ""
                + input.LT(3) != null ? " " + getTokenErrorDisplay(input.LT(3)) : "";

        } else if (e instanceof MismatchedTokenException) {
            MismatchedTokenException mte = (MismatchedTokenException) e;
            msg = super.getErrorMessage(e, xlateNames) + (input.LT(-1) == null ? "":" near '" + input.LT(-1).getText()) + "'"
            + ". Please refer to SQL document and check if there is any keyword conflict.";
        } else if (e instanceof FailedPredicateException) {
            FailedPredicateException fpe = (FailedPredicateException) e;
            msg = "Failed to recognize predicate '" + fpe.token.getText() + "'. Failed rule: '" + fpe.ruleName + "'";
        } else {
            if(xlateMap.containsKey("KW_"+e.token.getText().toUpperCase())){
                msg = e.token.getText() + " is a key word. Please refer to SQL document and check whether it can be used here or not.";
            } else {
                msg = super.getErrorMessage(e, xlateNames);
            }
        }

        if (msgs.size() > 0) {
            msg = msg + " in " + msgs.peek();
        }
        return msg;
    }

    // counter to generate unique union aliases


}


@rulecatch {
catch (RecognitionException e) {
 reportError(e);
  throw e;
}
}

// starting rule
statement
	: execStatement (SEMICOLON)? EOF
	;

integer
    : NegativeInteger
    | NonNegativeInteger
    ;

number
    : integer | Float | Boolean
    ;

numberOrString // identifier is string or integer
    : Boolean | Float | identifier
    ;

numberOrStringWidely
    : number
    | StringLiteral
    ;

execStatement
    : authorStatement
    | deleteStatement
    | updateStatement
    | insertStatement
    | queryStatement
    | metadataStatement
    | mergeStatement
//    | loadStatement
    | indexStatement
    | quitStatement
    | listStatement
    ;



dateFormat
    : datetime=DATETIME -> ^(TOK_DATETIME $datetime)
    | func=Identifier LPAREN RPAREN -> ^(TOK_DATETIME $func)
    ;

dateFormatWithNumber
    : dateFormat -> dateFormat
    | integer -> integer
    ;


/*
****
*************
metadata
*************
****
*/


metadataStatement
    : createTimeseries
    | setStorageGroup
    | addAPropertyTree
    | addALabelProperty
    | deleteALebelFromPropertyTree
    | linkMetadataToPropertyTree
    | unlinkMetadataNodeFromPropertyTree
    | deleteTimeseries
    | showMetadata
    | describePath
    ;

describePath
    : KW_DESCRIBE prefixPath
    -> ^(TOK_DESCRIBE prefixPath)
    ;

showMetadata
  : KW_SHOW KW_METADATA
  -> ^(TOK_SHOW_METADATA)
  ;

createTimeseries
  : KW_CREATE KW_TIMESERIES timeseries KW_WITH propertyClauses
  -> ^(TOK_CREATE ^(TOK_TIMESERIES timeseries) ^(TOK_WITH propertyClauses))
  ;

timeseries
  : KW_ROOT (DOT identifier)+
  -> ^(TOK_PATH ^(TOK_ROOT identifier+))
  ;

propertyClauses
  : KW_DATATYPE EQUAL propertyName=identifier COMMA KW_ENCODING EQUAL pv=propertyValue (COMMA propertyClause)*
  -> ^(TOK_DATATYPE $propertyName) ^(TOK_ENCODING $pv) propertyClause*
  ;

propertyClause
  : propertyName=identifier EQUAL pv=propertyValue
  -> ^(TOK_CLAUSE $propertyName $pv)
  ;

propertyValue
  : numberOrString
  ;

setStorageGroup
  : KW_SET KW_STORAGE KW_GROUP KW_TO prefixPath
  -> ^(TOK_SET ^(TOK_STORAGEGROUP prefixPath))
  ;

addAPropertyTree
  : KW_CREATE KW_PROPERTY property=identifier
  -> ^(TOK_CREATE ^(TOK_PROPERTY $property))
  ;

addALabelProperty
  : KW_ADD KW_LABEL label=identifier KW_TO KW_PROPERTY property=identifier
  -> ^(TOK_ADD ^(TOK_LABEL $label) ^(TOK_PROPERTY $property))
  ;

deleteALebelFromPropertyTree
  : KW_DELETE KW_LABEL label=identifier KW_FROM KW_PROPERTY property=identifier
  -> ^(TOK_DELETE ^(TOK_LABEL $label) ^(TOK_PROPERTY $property))
  ;

linkMetadataToPropertyTree
  : KW_LINK prefixPath KW_TO propertyPath
  -> ^(TOK_LINK prefixPath propertyPath)
  ;


propertyPath
  : property=identifier DOT label=identifier
  -> ^(TOK_LABEL $label) ^(TOK_PROPERTY $property)
  ;

unlinkMetadataNodeFromPropertyTree
  :KW_UNLINK prefixPath KW_FROM propertyPath
  -> ^(TOK_UNLINK prefixPath  propertyPath)
  ;

deleteTimeseries
  : KW_DELETE KW_TIMESERIES prefixPath (COMMA prefixPath)*
  -> ^(TOK_DELETE ^(TOK_TIMESERIES prefixPath+))
  ;


/*
****
*************
crud & author
*************
****
*/
mergeStatement
    :
    KW_MERGE
    -> ^(TOK_MERGE)
    ;

quitStatement
    :
    KW_QUIT
    -> ^(TOK_QUIT)
    ;

queryStatement
   :
   selectClause
   whereClause?
   specialClause?
   -> ^(TOK_QUERY selectClause whereClause? specialClause?)
   ;

specialClause
    :
    limitClause slimitClause? -> limitClause slimitClause?
    |slimitClause limitClause? -> slimitClause limitClause?
    |(groupbyClause limitClause)=>groupbyClause limitClause slimitClause? -> groupbyClause limitClause slimitClause?
    |(groupbyClause slimitClause)=>groupbyClause slimitClause limitClause? -> groupbyClause slimitClause limitClause?
    |groupbyClause -> groupbyClause
    |fillClause slimitClause? -> fillClause slimitClause?
    ;

authorStatement
    : createUser
    | dropUser
    | createRole
    | dropRole
    | grantUser
    | grantRole
    | revokeUser
    | revokeRole
    | grantRoleToUser
    | revokeRoleFromUser
    ;

loadStatement
    : KW_LOAD KW_TIMESERIES (fileName=StringLiteral) identifier (DOT identifier)*
    -> ^(TOK_LOAD $fileName identifier+)
    ;

createUser
    : KW_CREATE KW_USER
        userName=Identifier
        password=numberOrString
    -> ^(TOK_CREATE ^(TOK_USER $userName) ^(TOK_PASSWORD $password ))
    ;

dropUser
    : KW_DROP KW_USER userName=Identifier
    -> ^(TOK_DROP ^(TOK_USER $userName))
    ;

createRole
    : KW_CREATE KW_ROLE roleName=Identifier
    -> ^(TOK_CREATE ^(TOK_ROLE $roleName))
    ;

dropRole
    : KW_DROP KW_ROLE roleName=Identifier
    -> ^(TOK_DROP ^(TOK_ROLE $roleName))
    ;

grantUser
    : KW_GRANT KW_USER userName = Identifier privileges KW_ON prefixPath
    -> ^(TOK_GRANT ^(TOK_USER $userName) privileges prefixPath)
    ;

grantRole
    : KW_GRANT KW_ROLE roleName=Identifier privileges KW_ON prefixPath
    -> ^(TOK_GRANT ^(TOK_ROLE $roleName) privileges prefixPath)
    ;

revokeUser
    : KW_REVOKE KW_USER userName = Identifier privileges KW_ON prefixPath
    -> ^(TOK_REVOKE ^(TOK_USER $userName) privileges prefixPath)
    ;

revokeRole
    : KW_REVOKE KW_ROLE roleName = Identifier privileges KW_ON prefixPath
    -> ^(TOK_REVOKE ^(TOK_ROLE $roleName) privileges prefixPath)
    ;

grantRoleToUser
    : KW_GRANT roleName = Identifier KW_TO userName = Identifier
    -> ^(TOK_GRANT ^(TOK_ROLE $roleName) ^(TOK_USER $userName))
    ;

revokeRoleFromUser
    : KW_REVOKE roleName = Identifier KW_FROM userName = Identifier
    -> ^(TOK_REVOKE ^(TOK_ROLE $roleName) ^(TOK_USER $userName))
    ;

privileges
    : KW_PRIVILEGES StringLiteral (COMMA StringLiteral)*
    -> ^(TOK_PRIVILEGES StringLiteral+)
    ;

listStatement
    : KW_LIST KW_USER
    -> ^(TOK_LIST TOK_USER)
    | KW_LIST KW_ROLE
    -> ^(TOK_LIST TOK_ROLE)
    | KW_LIST KW_PRIVILEGES KW_USER username = Identifier KW_ON prefixPath
    -> ^(TOK_LIST TOK_PRIVILEGES ^(TOK_USER $username) prefixPath)
    | KW_LIST KW_PRIVILEGES KW_ROLE roleName = Identifier KW_ON prefixPath
    -> ^(TOK_LIST TOK_PRIVILEGES ^(TOK_ROLE $roleName) prefixPath)
    | KW_LIST KW_USER KW_PRIVILEGES username = Identifier
    -> ^(TOK_LIST TOK_PRIVILEGES TOK_ALL ^(TOK_USER $username))
    | KW_LIST KW_ROLE KW_PRIVILEGES roleName = Identifier
    -> ^(TOK_LIST TOK_PRIVILEGES TOK_ALL ^(TOK_ROLE $roleName))
    | KW_LIST KW_ALL KW_ROLE KW_OF KW_USER username = Identifier
    -> ^(TOK_LIST TOK_ROLE TOK_ALL ^(TOK_USER $username))
    | KW_LIST KW_ALL KW_USER KW_OF KW_ROLE roleName = Identifier
    -> ^(TOK_LIST TOK_USER TOK_ALL ^(TOK_ROLE $roleName))
    ;

prefixPath
    : KW_ROOT (DOT nodeName)*
    -> ^(TOK_PATH ^(TOK_ROOT nodeName*))
    ;

suffixPath
    : nodeName (DOT nodeName)*
      -> ^(TOK_PATH nodeName+)
    ;

nodeName
    : identifier
    | STAR
    ;

insertStatement
   : KW_INSERT KW_INTO prefixPath multidentifier KW_VALUES multiValue
   -> ^(TOK_INSERT prefixPath multidentifier multiValue)
   ;

/*
Assit to multi insert, target grammar:  insert into root.<deviceType>.<deviceName>(time, s1 ,s2) values(timeV, s1V, s2V)
*/

multidentifier
	:
	LPAREN KW_TIMESTAMP (COMMA Identifier)* RPAREN
	-> ^(TOK_MULT_IDENTIFIER TOK_TIME Identifier*)
	;
multiValue
	:
	LPAREN time=dateFormatWithNumber (COMMA numberOrStringWidely)* RPAREN
	-> ^(TOK_MULT_VALUE $time numberOrStringWidely*)
	;


deleteStatement
   :
   KW_DELETE KW_FROM prefixPath (COMMA prefixPath)* (whereClause)?
   -> ^(TOK_DELETE prefixPath+ whereClause?)
   ;

updateStatement
   : KW_UPDATE prefixPath (COMMA prefixPath)* KW_SET setClause (whereClause)?
   -> ^(TOK_UPDATE prefixPath+ setClause whereClause?)
   | KW_UPDATE KW_USER userName=Identifier KW_SET KW_PASSWORD psw=numberOrString
   -> ^(TOK_UPDATE ^(TOK_UPDATE_PSWD $userName $psw))
   ;

setClause
    : setExpression (COMMA setExpression)*
    -> setExpression+
    ;

setExpression
    : suffixPath EQUAL numberOrStringWidely
    -> ^(TOK_VALUE suffixPath numberOrStringWidely)
    ;


/*
****
*************
Index Statment
*************
****
*/

indexStatement
    : createIndexStatement
    | dropIndexStatement
    ;

createIndexStatement
    : KW_CREATE KW_INDEX KW_ON p=timeseries KW_USING func=Identifier indexWithClause? whereClause?
    -> ^(TOK_CREATE ^(TOK_INDEX $p ^(TOK_FUNC $func indexWithClause? whereClause?)))
    ;


indexWithClause
    : KW_WITH indexWithEqualExpression (COMMA indexWithEqualExpression)?
    -> ^(TOK_WITH indexWithEqualExpression+)
    ;

indexWithEqualExpression
    : k=Identifier EQUAL v=integer
    -> ^(TOK_INDEX_KV $k $v)
    ;

//indexWhereClause
//    : KW_WHERE name=Identifier GREATERTHAN value=dateFormatWithNumber
//    -> ^(TOK_WHERE $name $value)
//    ;


dropIndexStatement
    : KW_DROP KW_INDEX func=Identifier KW_ON p=timeseries
    -> ^(TOK_DROP ^(TOK_INDEX $p ^(TOK_FUNC $func)))
    ;

/*
****
*************
Basic Blocks
*************
****
*/


identifier
    :
    Identifier | integer
    ;

//selectClause
//    : KW_SELECT path (COMMA path)*
//    -> ^(TOK_SELECT path+)
//    | KW_SELECT clstcmd = identifier LPAREN path RPAREN (COMMA clstcmd=identifier LPAREN path RPAREN)*
//    -> ^(TOK_SELECT ^(TOK_CLUSTER path $clstcmd)+ )
//    ;

selectClause
    : KW_SELECT KW_INDEX func=Identifier LPAREN p1=timeseries COMMA p2=timeseries COMMA n1=dateFormatWithNumber COMMA n2=dateFormatWithNumber COMMA epsilon=Float (COMMA alpha=Float COMMA beta=Float)? RPAREN (fromClause)?
    -> ^(TOK_SELECT_INDEX $func $p1 $p2 $n1 $n2 $epsilon ($alpha $beta)?) fromClause?
    | KW_SELECT clusteredPath (COMMA clusteredPath)* fromClause
    -> ^(TOK_SELECT clusteredPath+) fromClause
    ;

clusteredPath
	: clstcmd = identifier LPAREN suffixPath RPAREN
	-> ^(TOK_PATH ^(TOK_CLUSTER suffixPath $clstcmd) )
	| suffixPath
	-> suffixPath
	;

fromClause
    :
    KW_FROM prefixPath (COMMA prefixPath)* -> ^(TOK_FROM prefixPath+)
    ;


whereClause
    :
    KW_WHERE searchCondition -> ^(TOK_WHERE searchCondition)
    ;

groupbyClause
    :
    KW_GROUP KW_BY LPAREN value=integer unit=Identifier (COMMA timeOrigin=dateFormatWithNumber)? COMMA timeInterval (COMMA timeInterval)* RPAREN
    -> ^(TOK_GROUPBY ^(TOK_TIMEUNIT $value $unit) ^(TOK_TIMEORIGIN $timeOrigin)? ^(TOK_TIMEINTERVAL timeInterval+))
    ;

fillClause
    :
    KW_FILL LPAREN typeClause (COMMA typeClause)* RPAREN
    -> ^(TOK_FILL typeClause+)
    ;

limitClause
    :
    KW_LIMIT N=NonNegativeInteger offsetClause?
    -> ^(TOK_LIMIT $N)
    ;

offsetClause
    :
    KW_OFFSET OFFSETValue=NonNegativeInteger
    ;

slimitClause
    :
    KW_SLIMIT SN=NonNegativeInteger soffsetClause?
    -> ^(TOK_SLIMIT $SN) soffsetClause?
    ;

soffsetClause
    :
    KW_SOFFSET SOFFSETValue=NonNegativeInteger
    -> ^(TOK_SOFFSET $SOFFSETValue)
    ;

typeClause
    : type=Identifier LSQUARE c=interTypeClause RSQUARE
    -> ^(TOK_TYPE $type $c)
    ;

interTypeClause
    :
    KW_LINEAR (COMMA value1=integer unit1=Identifier COMMA value2=integer unit2=Identifier)?
    -> ^(TOK_LINEAR (^(TOK_TIMEUNIT $value1 $unit1) ^(TOK_TIMEUNIT $value2 $unit2))?)
    |
    KW_PREVIOUS (COMMA value1=integer unit1=Identifier)?
    -> ^(TOK_PREVIOUS ^(TOK_TIMEUNIT $value1 $unit1)?)
    ;



timeInterval
    :
    LSQUARE startTime=dateFormatWithNumber COMMA endTime=dateFormatWithNumber RSQUARE
    -> ^(TOK_TIMEINTERVALPAIR $startTime $endTime)
    ;

searchCondition
    :
    expression
    ;

expression
    :
    precedenceOrExpression
    ;

precedenceOrExpression
    :
    precedenceAndExpression ( KW_OR^ precedenceAndExpression)*
    ;

precedenceAndExpression
    :
    precedenceNotExpression ( KW_AND^ precedenceNotExpression)*
    ;

precedenceNotExpression
    :
    (KW_NOT^)* precedenceEqualExpressionSingle
    ;


precedenceEqualExpressionSingle
    :
    (left=atomExpression -> $left)
    (
    	(precedenceEqualOperator equalExpr=atomExpression)
       -> ^(precedenceEqualOperator $precedenceEqualExpressionSingle $equalExpr)
    )*
    ;


precedenceEqualOperator
    :
    EQUAL | EQUAL_NS | NOTEQUAL | LESSTHANOREQUALTO | LESSTHAN | GREATERTHANOREQUALTO | GREATERTHAN
    ;



nullCondition
    :
    KW_NULL -> ^(TOK_ISNULL)
    | KW_NOT KW_NULL -> ^(TOK_ISNOTNULL)
    ;



atomExpression
    :
    (KW_NULL) => KW_NULL -> TOK_NULL
    | (KW_TIME) => KW_TIME -> ^(TOK_PATH KW_TIME)
    | (constant) => constant
    | prefixPath
    | suffixPath
    | LPAREN! expression RPAREN!
    ;

constant
    : number
    | StringLiteral
    | dateFormat
    ;