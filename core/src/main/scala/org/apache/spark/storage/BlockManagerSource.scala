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

package org.apache.spark.storage

import com.codahale.metrics.{Gauge, MetricRegistry}

import org.apache.spark.metrics.source.Source

import org.apache.hadoop.fs.FileSystem

private[spark] class BlockManagerSource(val blockManager: BlockManager)
    extends Source {
  override val metricRegistry = new MetricRegistry()
  override val sourceName = "BlockManager"

  metricRegistry.register(MetricRegistry.name("memory", "maxMem_MB"), new Gauge[Long] {
    override def getValue: Long = {
      val storageStatusList = blockManager.master.getStorageStatus
      val maxMem = storageStatusList.map(_.maxMem).sum
      maxMem / 1024 / 1024
    }
  })

  metricRegistry.register(MetricRegistry.name("memory", "remainingMem_MB"), new Gauge[Long] {
    override def getValue: Long = {
      val storageStatusList = blockManager.master.getStorageStatus
      val remainingMem = storageStatusList.map(_.memRemaining).sum
      remainingMem / 1024 / 1024
    }
  })

  metricRegistry.register(MetricRegistry.name("memory", "memUsed_MB"), new Gauge[Long] {
    override def getValue: Long = {
      val storageStatusList = blockManager.master.getStorageStatus
      val memUsed = storageStatusList.map(_.memUsed).sum
      memUsed / 1024 / 1024
    }
  })

  metricRegistry.register(MetricRegistry.name("disk", "diskSpaceUsed_MB"), new Gauge[Long] {
    override def getValue: Long = {
      val storageStatusList = blockManager.master.getStorageStatus
      val diskSpaceUsed = storageStatusList.map(_.diskUsed).sum
      diskSpaceUsed / 1024 / 1024
    }
  })

  metricRegistry.register(MetricRegistry.name("blocks", "requested"), new Gauge[Long] {
    override def getValue: Long = {
      blockManager.getBlocksRequested
    }
  })

  metricRegistry.register(MetricRegistry.name("blocks", "foundInMemory"), new Gauge[Long] {
    override def getValue: Long = {
      blockManager.getBlocksFoundInMemory
    }
  })

  metricRegistry.register(MetricRegistry.name("blocks", "missesInMemory"), new Gauge[Long] {
    override def getValue: Long = {
      blockManager.getMissesInMemory
    }
  })

  metricRegistry.register(MetricRegistry.name("blocks", "foundInExternalStore"), new Gauge[Long] {
    override def getValue: Long = {
      blockManager.getBlocksFoundInExternalStore
    }
  })

  metricRegistry.register(MetricRegistry.name("blocks", "foundOnDisk"), new Gauge[Long] {
    override def getValue: Long = {
      blockManager.getBlocksFoundOnDisk
    }
  })

  metricRegistry.register(MetricRegistry.name("blocks", "droppedFromMemory"), new Gauge[Long] {
    override def getValue: Long = {
      blockManager.getBlocksDroppedFromMemory
    }
  })

  metricRegistry.register(MetricRegistry.name("blocks", "droppedToDisk"), new Gauge[Long] {
    override def getValue: Long = {
      blockManager.getBlocksDroppedToDisk
    }
  })

  metricRegistry.register(MetricRegistry.name("blocks", "notAttempted"), new Gauge[Long] {
    override def getValue: Long = {
      blockManager.getBlocksNotAttempted
    }
  })

  metricRegistry.register(MetricRegistry.name("fs", "fileOpenOps"), new Gauge[Long] {
    override def getValue: Long = {
      FileSystem.getAllStatistics().get(0).getFileOpenOps()  
    }
  })

  metricRegistry.register(MetricRegistry.name("fs", "fileCloseOps"), new Gauge[Long] {
    override def getValue: Long = {
      FileSystem.getAllStatistics().get(0).getFileCloseOps()  
    }
  })

  metricRegistry.register(MetricRegistry.name("fs", "seekOps"), new Gauge[Long] {
    override def getValue: Long = {
      FileSystem.getAllStatistics().get(0).getSeekOps()  
    }
  })

  metricRegistry.register(MetricRegistry.name("fs", "metadataTime"), new Gauge[Long] {
    override def getValue: Long = {
      FileSystem.getAllStatistics().get(0).getMetadataTime()  
    }
  })

}
