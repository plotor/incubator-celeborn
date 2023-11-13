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

package org.apache.spark.shuffle.celeborn

import org.apache.celeborn.client.LifecycleManager
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging

class CelebornShuffleFallbackPolicyRunner(conf: CelebornConf) extends Logging {

  def applyAllFallbackPolicy(lifecycleManager: LifecycleManager, numPartitions: Int): Boolean = {
    applyForceFallbackPolicy() || // 配置启用强制 fallback
      applyShufflePartitionsFallbackPolicy(numPartitions) || // 分区数大于阈值（默认 500000）
      !checkQuota(lifecycleManager) || // 超出 Quota
      !checkWorkersAvailable(lifecycleManager) // 没有可用的 Worker
  }

  /**
   * if celeborn.shuffle.forceFallback.enabled is true, fallback to external shuffle
   * @return return celeborn.shuffle.forceFallback.enabled
   */
  def applyForceFallbackPolicy(): Boolean = {
    if (conf.shuffleForceFallbackEnabled) {
      val conf = CelebornConf.SPARK_SHUFFLE_FORCE_FALLBACK_ENABLED
      logWarning(s"${conf.alternatives.foldLeft(conf.key)((x, y) => s"$x or $y")} is enabled, which will force fallback.")
    }
    conf.shuffleForceFallbackEnabled
  }

  /**
   * if shuffle partitions > celeborn.shuffle.forceFallback.numPartitionsThreshold, fallback to external shuffle
   * @param numPartitions shuffle partitions
   * @return return if shuffle partitions bigger than limit
   */
  def applyShufflePartitionsFallbackPolicy(numPartitions: Int): Boolean = {
    val confNumPartitions = conf.shuffleForceFallbackPartitionThreshold
    val needFallback = numPartitions >= confNumPartitions
    if (needFallback) {
      logWarning(s"Shuffle num of partitions: $numPartitions" +
        s" is bigger than the limit: $confNumPartitions," +
        s" need fallback to spark shuffle")
    }
    needFallback
  }

  /**
   * If celeborn cluster is exceed current user's quota, fallback to external shuffle
   *
   * @return if celeborn cluster have available space for current user
   */
  def checkQuota(lifecycleManager: LifecycleManager): Boolean = {
    if (!conf.quotaEnabled) {
      return true
    }

    val resp = lifecycleManager.checkQuota()
    if (!resp.isAvailable) {
      logWarning(
        s"Quota exceed for current user ${lifecycleManager.getUserIdentifier}. Because: ${resp.reason}")
    }
    resp.isAvailable
  }

  /**
   * If celeborn cluster has no available workers, fallback to external shuffle.
   *
   * @return if celeborn cluster has available workers.
   */
  def checkWorkersAvailable(lifecycleManager: LifecycleManager): Boolean = {
    val resp = lifecycleManager.checkWorkersAvailable()
    if (!resp.getAvailable) {
      logWarning(s"No workers available for current user ${lifecycleManager.getUserIdentifier}.")
    }
    resp.getAvailable
  }
}
