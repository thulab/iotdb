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
package org.apache.iotdb.db.auth.user;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.entity.User;
import org.apache.iotdb.db.concurrent.HashLock;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.utils.AuthUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class stores information of each user in a separate file within a directory, and cache them
 * in memory when a user is accessed.
 */
public abstract class BasicUserManager implements IUserManager {

  private static final Logger logger = LoggerFactory.getLogger(BasicUserManager.class);

  private Map<String, User> userMap;
  private IUserAccessor accessor;
  private HashLock lock;

  /**
   * BasicUserManager Constructor.
   *
   * @param accessor user accessor
   * @throws AuthException Authentication Exception
   */
  public BasicUserManager(IUserAccessor accessor) throws AuthException {
    this.userMap = new HashMap<>();
    this.accessor = accessor;
    this.lock = new HashLock();

    reset();
  }

  /**
   * Try to load admin. If it doesn't exist, automatically create one.
   */
  private void initAdmin() throws AuthException {
    User admin;
    try {
      admin = getUser(IoTDBConstant.ADMIN_NAME);
    } catch (AuthException e) {
      logger.warn("Cannot load admin because {}. Create a new one.", e.getMessage());
      admin = null;
    }

    if (admin == null) {
      createUser(IoTDBConstant.ADMIN_NAME, IoTDBConstant.ADMIN_PW);
    }
    logger.info("Admin initialized");
  }

  @Override
  public User getUser(String username) throws AuthException {
    lock.readLock(username);
    User user = userMap.get(username);
    try {
      if (user == null) {
        user = accessor.loadUser(username);
        if (user != null) {
          userMap.put(username, user);
        }
      }
    } catch (IOException e) {
      throw new AuthException(e);
    } finally {
      lock.readUnlock(username);
    }
    if (user != null) {
      user.lastActiveTime = System.currentTimeMillis();
    }
    return user;
  }

  @Override
  public boolean createUser(String username, String password) throws AuthException {
    AuthUtils.validateUsername(username);
    AuthUtils.validatePassword(password);

    User user = getUser(username);
    if (user != null) {
      return false;
    }
    lock.writeLock(username);
    try {
      user = new User(username, AuthUtils.encryptPassword(password));
      accessor.saveUser(user);
      userMap.put(username, user);
      return true;
    } catch (IOException e) {
      throw new AuthException(e);
    } finally {
      lock.writeUnlock(username);
    }
  }

  @Override
  public boolean deleteUser(String username) throws AuthException {
    lock.writeLock(username);
    try {
      if (accessor.deleteUser(username)) {
        userMap.remove(username);
        return true;
      } else {
        return false;
      }
    } catch (IOException e) {
      throw new AuthException(e);
    } finally {
      lock.writeUnlock(username);
    }
  }

  @Override
  public boolean grantPrivilegeToUser(String username, String path, int privilegeId)
      throws AuthException {
    AuthUtils.validatePrivilegeOnPath(path, privilegeId);
    lock.writeLock(username);
    try {
      User user = getUser(username);
      if (user == null) {
        throw new AuthException(String.format("No such user %s", username));
      }
      if (user.hasPrivilege(path, privilegeId)) {
        return false;
      }
      Set<Integer> privilegesCopy = new HashSet<>(user.getPrivileges(path));
      user.addPrivilege(path, privilegeId);
      try {
        accessor.saveUser(user);
      } catch (IOException e) {
        user.setPrivileges(path, privilegesCopy);
        throw new AuthException(e);
      }
      return true;
    } finally {
      lock.writeUnlock(username);
    }
  }

  @Override
  public boolean revokePrivilegeFromUser(String username, String path, int privilegeId)
      throws AuthException {
    AuthUtils.validatePrivilegeOnPath(path, privilegeId);
    lock.writeLock(username);
    try {
      User user = getUser(username);
      if (user == null) {
        throw new AuthException(String.format("No such user %s", username));
      }
      if (!user.hasPrivilege(path, privilegeId)) {
        return false;
      }
      user.removePrivilege(path, privilegeId);
      try {
        accessor.saveUser(user);
      } catch (IOException e) {
        user.addPrivilege(path, privilegeId);
        throw new AuthException(e);
      }
      return true;
    } finally {
      lock.writeUnlock(username);
    }
  }

  @Override
  public boolean updateUserPassword(String username, String newPassword) throws AuthException {
    try {
      AuthUtils.validatePassword(newPassword);
    } catch (AuthException e) {
      return false;
    }

    lock.writeLock(username);
    try {
      User user = getUser(username);
      if (user == null) {
        throw new AuthException(String.format("No such user %s", username));
      }
      String oldPassword = user.password;
      user.password = AuthUtils.encryptPassword(newPassword);
      try {
        accessor.saveUser(user);
      } catch (IOException e) {
        user.password = oldPassword;
        throw new AuthException(e);
      }
      return true;
    } finally {
      lock.writeUnlock(username);
    }
  }

  @Override
  public boolean grantRoleToUser(String roleName, String username) throws AuthException {
    lock.writeLock(username);
    try {
      User user = getUser(username);
      if (user == null) {
        throw new AuthException(String.format("No such user %s", username));
      }
      if (user.hasRole(roleName)) {
        return false;
      }
      user.roleList.add(roleName);
      try {
        accessor.saveUser(user);
      } catch (IOException e) {
        user.roleList.remove(roleName);
        throw new AuthException(e);
      }
      return true;
    } finally {
      lock.writeUnlock(username);
    }
  }

  @Override
  public boolean revokeRoleFromUser(String roleName, String username) throws AuthException {
    lock.writeLock(username);
    try {
      User user = getUser(username);
      if (user == null) {
        throw new AuthException(String.format("No such user %s", username));
      }
      if (!user.hasRole(roleName)) {
        return false;
      }
      user.roleList.remove(roleName);
      try {
        accessor.saveUser(user);
      } catch (IOException e) {
        user.roleList.add(roleName);
        throw new AuthException(e);
      }
      return true;
    } finally {
      lock.writeUnlock(username);
    }
  }

  @Override
  public void reset() throws AuthException {
    accessor.reset();
    userMap.clear();
    lock.reset();
    initAdmin();
  }

  @Override
  public List<String> listAllUsers() {
    List<String> rtlist = accessor.listAllUsers();
    rtlist.sort(null);
    return rtlist;
  }

}
