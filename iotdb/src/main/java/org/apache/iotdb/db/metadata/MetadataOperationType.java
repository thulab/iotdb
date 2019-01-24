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
package org.apache.iotdb.db.metadata;

public class MetadataOperationType {

  public static final String ADD_PATH_TO_MTREE = "0";
  public static final String DELETE_PATH_FROM_MTREE = "1";
  public static final String SET_STORAGE_LEVEL_TO_MTREE = "2";
  public static final String ADD_A_PTREE = "3";
  public static final String ADD_A_PATH_TO_PTREE = "4";
  public static final String DELETE_PATH_FROM_PTREE = "5";
  public static final String LINK_MNODE_TO_PTREE = "6";
  public static final String UNLINK_MNODE_FROM_PTREE = "7";
  public static final String ADD_INDEX_TO_PATH = "8";
  public static final String DELETE_INDEX_FROM_PATH = "9";
}
