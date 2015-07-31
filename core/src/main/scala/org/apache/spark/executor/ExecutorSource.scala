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

package org.apache.spark.executor

import java.util.concurrent.ThreadPoolExecutor

import scala.collection.JavaConversions._

import com.codahale.metrics.{Gauge, MetricRegistry}
import org.apache.hadoop.fs.FileSystem

import org.apache.spark.SparkEnv
import org.apache.spark.metrics.source.Source

private[spark]
class ExecutorSource(threadPool: ThreadPoolExecutor, executorId: String) extends Source {

  private def fileStats(scheme: String) : Option[FileSystem.Statistics] =
    FileSystem.getAllStatistics().find(s => s.getScheme.equals(scheme))

  private def registerFileSystemStat[T](
        scheme: String, name: String, f: FileSystem.Statistics => T, defaultValue: T) = {
    metricRegistry.register(MetricRegistry.name("filesystem", scheme, name), new Gauge[T] {
      override def getValue: T = fileStats(scheme).map(f).getOrElse(defaultValue)
    })
  }

  override val metricRegistry = new MetricRegistry()

  override val sourceName = "executor"

  // Gauge for executor thread pool's actively executing task counts
  metricRegistry.register(MetricRegistry.name("threadpool", "activeTasks"), new Gauge[Int] {
    override def getValue: Int = threadPool.getActiveCount()
  })

  // Gauge for executor thread pool's approximate total number of tasks that have been completed
  metricRegistry.register(MetricRegistry.name("threadpool", "completeTasks"), new Gauge[Long] {
    override def getValue: Long = threadPool.getCompletedTaskCount()
  })

  // Gauge for executor thread pool's current number of threads
  metricRegistry.register(MetricRegistry.name("threadpool", "currentPool_size"), new Gauge[Int] {
    override def getValue: Int = threadPool.getPoolSize()
  })

  // Gauge got executor thread pool's largest number of threads that have ever simultaneously
  // been in th pool
  metricRegistry.register(MetricRegistry.name("threadpool", "maxPool_size"), new Gauge[Int] {
    override def getValue: Int = threadPool.getMaximumPoolSize()
  })

  // Gauge for file system stats of this executor
  for (scheme <- Array("hdfs", "file")) {
    registerFileSystemStat(scheme, "read_bytes", _.getBytesRead(), 0L)
    registerFileSystemStat(scheme, "write_bytes", _.getBytesWritten(), 0L)
    registerFileSystemStat(scheme, "read_ops", _.getReadOps(), 0)
    registerFileSystemStat(scheme, "largeRead_ops", _.getLargeReadOps(), 0)
    registerFileSystemStat(scheme, "write_ops", _.getWriteOps(), 0)
    registerFileSystemStat(scheme, "file_open_ops", _.getFileOpenOps(), 0)
    registerFileSystemStat(scheme, "file_close_ops", _.getFileCloseOps(), 0)
    registerFileSystemStat(scheme, "seek_ops", _.getSeekOps(), 0)
    registerFileSystemStat(scheme, "metadata_time", _.getMetadataTime(), 0L)
  }

  metricRegistry.register(MetricRegistry.name("memoryStore", "usedMemory"), new Gauge[Long] {
    override def getValue: Long = {
      SparkEnv.get.blockManager.memoryStore.usedMemory
    }
  })

  metricRegistry.register(MetricRegistry.name("memoryStore", "freeMemory"), new Gauge[Long] {
    override def getValue: Long = {
      SparkEnv.get.blockManager.memoryStore.freeMemory
    }
  })

  metricRegistry.register(MetricRegistry.name("blocks", "requested"), new Gauge[Long] {
    override def getValue: Long = {
      SparkEnv.get.blockManager.getBlocksRequested
    }
  })

  metricRegistry.register(MetricRegistry.name("blocks", "foundInMemory"), new Gauge[Long] {
    override def getValue: Long = {
      SparkEnv.get.blockManager.getBlocksFoundInMemory
    }
  })

  metricRegistry.register(MetricRegistry.name("blocks", "missesInMemory"), new Gauge[Long] {
    override def getValue: Long = {
      SparkEnv.get.blockManager.getMissesInMemory
    }
  })

  metricRegistry.register(MetricRegistry.name("blocks", "foundInExternalStore"), new Gauge[Long] {
    override def getValue: Long = {
      SparkEnv.get.blockManager.getBlocksFoundInExternalStore
    }
  })

  metricRegistry.register(MetricRegistry.name("blocks", "foundOnDisk"), new Gauge[Long] {
    override def getValue: Long = {
      SparkEnv.get.blockManager.getBlocksFoundOnDisk
    }
  })

  metricRegistry.register(MetricRegistry.name("blocks", "droppedFromMemory"), new Gauge[Long] {
    override def getValue: Long = {
      SparkEnv.get.blockManager.getBlocksDroppedFromMemory
    }
  })

  metricRegistry.register(MetricRegistry.name("blocks", "droppedToDisk"), new Gauge[Long] {
    override def getValue: Long = {
      SparkEnv.get.blockManager.getBlocksDroppedToDisk
    }
  })

  metricRegistry.register(MetricRegistry.name("blocks", "notAttempted"), new Gauge[Long] {
    override def getValue: Long = {
      SparkEnv.get.blockManager.getBlocksNotAttempted
    }
  })

  metricRegistry.register(MetricRegistry.name("blocks", "removed"), new Gauge[Long] {
    override def getValue: Long = {
      SparkEnv.get.blockManager.getBlocksRemoved
    }
  })

  metricRegistry.register(MetricRegistry.name("memoryStore", "bytesPut"), new Gauge[Long] {
    override def getValue: Long = {
      SparkEnv.get.blockManager.getBytesPutInMemoryStore
    }
  })

  metricRegistry.register(MetricRegistry.name("memoryStore", "bytesRetrieved"), new Gauge[Long] {
    override def getValue: Long = {
      SparkEnv.get.blockManager.getBytesRetrievedFromMemoryStore
    }
  })

  metricRegistry.register(MetricRegistry.name("diskStore", "bytesPut"), new Gauge[Long] {
    override def getValue: Long = {
      SparkEnv.get.blockManager.getBytesPutInDiskStore
    }
  })

  metricRegistry.register(MetricRegistry.name("diskStore", "bytesRetrieved"), new Gauge[Long] {
    override def getValue: Long = {
      SparkEnv.get.blockManager.getBytesRetrievedFromDiskStore
    }
  })

  metricRegistry.register(MetricRegistry.name("blockObjectWriter", "bytesWritten"), new Gauge[Long] {
    override def getValue: Long = {
      SparkEnv.get.blockManager.getBlockObjectWriterBytesWritten
    }
  })

  metricRegistry.register(MetricRegistry.name("shuffleBlocks", "bytesRetrieved"), new Gauge[Long] {
    override def getValue: Long = {
      SparkEnv.get.blockManager.getShuffleBlockBytesRetrieved
    }
  })

  metricRegistry.register(MetricRegistry.name("shuffleBlocks", "cleanOps"), new Gauge[Long] {
    override def getValue: Long = {
      SparkEnv.get.blockManager.getShuffleCleanOps
    }
  })

  metricRegistry.register(MetricRegistry.name("cacheManager", "partitionsComputed"), new Gauge[Long] {
    override def getValue: Long = {
      SparkEnv.get.blockManager.getCacheManagerPartitionsComputed
    }
  })

  metricRegistry.register(MetricRegistry.name("cacheManager", "partitionsFound"), new Gauge[Long] {
    override def getValue: Long = {
      SparkEnv.get.blockManager.getCacheManagerPartitionsFound
    }
  })

}
