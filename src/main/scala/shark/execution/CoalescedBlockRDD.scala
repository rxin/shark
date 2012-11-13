package shark.execution

import spark._
import shark.Utils


// index is the map id. location is the slave node that has the block.
class CoalescedBlockSplit(
  val index: Int, val location: String, val numMapOutputs: Int) extends Split

class CoalescedBlockRDD[T: ClassManifest](sc: SparkContext, val shuffleId: Int)
  extends RDD[T](sc) {

  @transient
  val _splits : Array[Split] = {
    val mapOutputTracker = SparkEnv.get.mapOutputTracker
    val mapStatuses = mapOutputTracker.getServerStatuses(shuffleId)

    mapStatuses.zipWithIndex.map { case(mapStatus, mapId) =>
      new CoalescedBlockSplit(mapId, mapStatus.address.ip, mapStatus.compressedSizes.size)
    }
  }

  override def splits = _splits

  override def preferredLocations(split: Split): Seq[String] =
    Seq(split.asInstanceOf[CoalescedBlockSplit].location)

  // This is not fault-tolerant.
  override val dependencies: List[Dependency[_]] = Nil

  override def compute(split: Split): Iterator[T] = {
    val numMapOutputs = split.asInstanceOf[CoalescedBlockSplit].numMapOutputs
    val mapId = split.index
    val blockManager = SparkEnv.get.blockManager
    (0 until numMapOutputs).iterator.flatMap { reduceId: Int =>
      val blockId = "shuffle_%d_%d_%d".format(shuffleId, mapId, reduceId)
      blockManager.get(blockId) match {
        case Some(block) => block.asInstanceOf[Iterator[T]]
        case None =>
          throw new Exception("Could not compute split, block " + blockId + " not found")
      }
    }
  }
}
