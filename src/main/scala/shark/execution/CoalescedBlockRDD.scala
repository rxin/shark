package shark.execution

import spark._
import shark.Utils


// index is the map id. location is the slave node that has the block.
class CoalescedBlockSplit(
  val index: Int, val location: String) extends Split

class CoalescedBlockRDD[T: ClassManifest](
    sc: SparkContext,
    val shuffleId: Int,
    val numMapTasks: Int,
    val numMapOutputs: Int)
  extends RDD[T](sc) {

  @transient
  val _splits : Array[Split] = {
    // Use the first reduce block to get the preferred location for a given map task.
    val testingBlocks = Array.tabulate(numMapTasks)(mapId => "shuffle_%d_%d_%d".format(
      shuffleId, mapId, 0))
    val locations: Array[Seq[String]] = SparkEnv.get.blockManager.getLocations(testingBlocks)

    Array.tabulate(numMapTasks)(mapId => new CoalescedBlockSplit(mapId, locations(mapId).head))
  }

  override def splits = _splits

  override def preferredLocations(split: Split): Seq[String] =
    Seq(split.asInstanceOf[CoalescedBlockSplit].location)

  // This is not fault-tolerant.
  override val dependencies: List[Dependency[_]] = Nil

  override def compute(split: Split): Iterator[T] = {
    val mapId = split.index
    val blockManager = SparkEnv.get.blockManager
    (0 until numMapOutputs).iterator.flatMap { reduceId: Int =>
      val blockId = "shuffle_%d_%d_%d".format(shuffleId, mapId, reduceId)
      blockManager.getLocal(blockId) match {
        case Some(block) => block.asInstanceOf[Iterator[T]]
        case None =>
          throw new Exception("Could not compute split, block " + blockId + " not found")
      }
    }
  }
}
