package spark

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

import spark.storage.BlockManagerId
import spark.scheduler.ShuffleBlockStatus


object BlockFetcher extends Logging {

  def fetchMultiple[K, V](shuffleId: Int, reduceIds: Array[Int]) : Iterator[(K, V)] = {
    logDebug("Fetching outputs for shuffle %d, reduce %s".format(shuffleId, reduceIds))
    val blockManager = SparkEnv.get.blockManager

    val startTime = System.currentTimeMillis

    // Array of (reduce id, map id, block manager id, size of the block)
    val statuses: Array[(Int, Int, BlockManagerId, Long)] = reduceIds.flatMap { reduceId =>
      SparkEnv.get.mapOutputTracker.getServerStatuses(shuffleId, reduceId).zipWithIndex.map {
        case(s, mapId) => (reduceId, mapId, s.address, s.size)
      }
    }

    logDebug("Fetching map output location for shuffle %d, reduce %s took %d ms".format(
      shuffleId, reduceIds, System.currentTimeMillis - startTime))

    // Map from block manager to the list of blocks we need to fetch from the block manager.
    // The map value is a list of tuples (reduce id, map id, size)
    val splitsByAddress = new HashMap[BlockManagerId, ArrayBuffer[(Int, Int, Long)]]
    for ((reduceId, mapId, address, size) <- statuses) {
      splitsByAddress.getOrElseUpdate(address, ArrayBuffer()) += ((reduceId, mapId, size))
    }

    // Generate the sequence of blocks to fetch for each block manager.
    // (String, Long) = (block name, size of the block)
    val blocksByAddress: Seq[(BlockManagerId, Seq[(String, Long)])] = splitsByAddress.toSeq.map {
      case (address, splits) =>
        // shuffle id, map id, reduce id
        (address, splits.map(s => ("shuffle_%d_%d_%d".format(shuffleId, s._2, s._1), s._3)))
    }

    def unpackBlock(blockPair: (String, Option[Iterator[Any]])) : Iterator[(K, V)] = {
      val blockId = blockPair._1
      val blockOption = blockPair._2
      blockOption match {
        case Some(block) => {
          block.asInstanceOf[Iterator[(K, V)]]
        }
        case None => {
          val regex = "shuffle_([0-9]*)_([0-9]*)_([0-9]*)".r
          blockId match {
            case regex(shufId, mapId, reduceId) =>
              val address = statuses(mapId.toInt)._3
              throw new FetchFailedException(
                address, shufId.toInt, mapId.toInt, reduceId.toInt, null)
            case _ =>
              throw new SparkException(
                "Failed to get block " + blockId + ", which is not a shuffle block")
          }
        }
      }
    }
    blockManager.getMultiple(blocksByAddress).flatMap(unpackBlock)
  }
}
