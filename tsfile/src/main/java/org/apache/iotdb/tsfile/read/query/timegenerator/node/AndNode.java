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
package org.apache.iotdb.tsfile.read.query.timegenerator.node;

import java.io.IOException;

public class AndNode implements Node {

  private Node leftChild;
  private Node rightChild;

  private long cachedValue;
  private boolean hasCachedValue;

  /**
   * Constructor of AndNode.
   *
   * @param leftChild left child
   * @param rightChild right child
   */
  public AndNode(Node leftChild, Node rightChild) {
    this.leftChild = leftChild;
    this.rightChild = rightChild;
    this.hasCachedValue = false;
  }

  @Override
  public boolean hasNext() throws IOException {
    if (hasCachedValue) {
      return true;
    }
    if (leftChild.hasNext() && rightChild.hasNext()) {
      long leftValue = leftChild.next();
      long rightValue = rightChild.next();
      while (true) {
        if (leftValue == rightValue) {
          this.hasCachedValue = true;
          this.cachedValue = leftValue;
          return true;
        } else if (leftValue > rightValue) {
          if (rightChild.hasNext()) {
            rightValue = rightChild.next();
          } else {
            return false;
          }
        } else { // leftValue < rightValue
          if (leftChild.hasNext()) {
            leftValue = leftChild.next();
          } else {
            return false;
          }
        }
      }
    }
    return false;
  }

  /**
   * If there is no value in current Node, -1 will be returned if {@code next()} is invoked.
   */
  @Override
  public long next() throws IOException {
    if (hasNext()) {
      hasCachedValue = false;
      return cachedValue;
    }
    return -1;
  }

  @Override
  public NodeType getType() {
    return NodeType.AND;
  }
}
