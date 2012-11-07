package shark

import java.util.Locale

object Utils {

  /**
   * Convert a memory quantity in bytes to a human-readable string such as "4.0 MB".
   */
  def memoryBytesToString(size: Long): String = {
    val TB = 1L << 40
    val GB = 1L << 30
    val MB = 1L << 20
    val KB = 1L << 10

    val (value, unit) = {
      if (size >= 2*TB) {
        (size.asInstanceOf[Double] / TB, "TB")
      } else if (size >= 2*GB) {
        (size.asInstanceOf[Double] / GB, "GB")
      } else if (size >= 2*MB) {
        (size.asInstanceOf[Double] / MB, "MB")
      } else if (size >= 2*KB) {
        (size.asInstanceOf[Double] / KB, "KB")
      } else {
        (size.asInstanceOf[Double], "B")
      }
    }
    "%.1f %s".formatLocal(Locale.US, value, unit)
  }

  /**
   * A hack to remove all shuffle data from block manager.
   */
  def removeShuffleData(numTasks: Int) {
    import scala.collection.JavaConversions._
    SharkEnv.sc.parallelize(0 until numTasks, numTasks).foreach { task =>
      spark.SparkEnv.get.shuffleBlocks.synchronized {
        val shuffleBlocks = spark.SparkEnv.get.shuffleBlocks
        val blockManager = spark.SparkEnv.get.blockManager

        shuffleBlocks.foreach { case(shuffleId, info) =>
          val numMapOutputs: Int = info._1
          val mapIds: collection.mutable.ArrayBuffer[Int] = info._2

          var i = 0
          while (i < mapIds.size) {
            var j = 0
            while (j < numMapOutputs) {
              blockManager.drop("shuffle_" + shuffleId + "_" + mapIds(i) + "_" + j)
              j += 1
            }
            i += 1
          }
        }

        shuffleBlocks.clear()
      }
    }
  }

}
