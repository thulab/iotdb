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
package org.apache.iotdb.db.auth.authorizer;

import java.io.File;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.role.LocalFileRoleManager;
import org.apache.iotdb.db.auth.user.LocalFileUserManager;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalFileAuthorizer extends BasicAuthorizer {

  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static Logger logger = LoggerFactory.getLogger(LocalFileAuthorizer.class);

  private LocalFileAuthorizer() throws AuthException {
    super(new LocalFileUserManager(config.dataDir + File.separator + "users" + File.separator),
        new LocalFileRoleManager(config.dataDir + File.separator + "roles" + File.separator));
  }

  /**
   * function for getting the instance of the local file authorizer.
   */
  public static LocalFileAuthorizer getInstance() throws AuthException {
    if (InstanceHolder.instance == null) {
      throw new AuthException("Authorizer uninitialized");
    }
    return InstanceHolder.instance;
  }

  private static class InstanceHolder {

    private static LocalFileAuthorizer instance;

    static {
      try {
        instance = new LocalFileAuthorizer();
      } catch (AuthException e) {
        logger.error("Authorizer initialization failed due to ", e);
        instance = null;
      }
    }
  }
}
