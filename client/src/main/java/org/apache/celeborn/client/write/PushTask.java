/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.celeborn.client.write;

public class PushTask {
  /* 分区 ID */
  private int partitionId;
  /* 数据长度 */
  private int size;

  /* 缓冲区 */
  private byte[] buffer;

  public PushTask(int bufferSize) {
    this.buffer = new byte[bufferSize];
  }

  public int getPartitionId() {
    return partitionId;
  }

  public void setPartitionId(int partitionId) {
    this.partitionId = partitionId;
  }

  public int getSize() {
    return size;
  }

  public void setSize(int size) {
    if (size > buffer.length) {
      buffer = new byte[size];
    }
    this.size = size;
  }

  public byte[] getBuffer() {
    return buffer;
  }
}
