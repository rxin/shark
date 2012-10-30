package spark

import java.util.{HashMap => JHashMap}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

// A version of CoGroupedRDD with the following changes:
// - Disable map-side aggregation.
// - Enforce return type to Array[ArrayBuffer].

sealed trait CoGroupSplitDep extends Serializable
case class NarrowCoGroupSplitDep(rdd: RDD[_], split: Split) extends CoGroupSplitDep
case class ShuffleCoGroupSplitDep(shuffleId: Int) extends CoGroupSplitDep



class CoGroupSplit(
  idx: Int,
  val deps: Seq[CoGroupSplitDep],
  val partitions: Array[Int])
extends Split with Serializable {

  override val index: Int = idx
  override def hashCode(): Int = idx
}

class CoGroupedRDD[K](
    @transient rdds: Seq[RDD[(_, _)]],
    part: Partitioner,
    @transient splits_ : Array[Split],
    @transient val dependencies: List[Dependency[_]])
  extends RDD[(K, Array[ArrayBuffer[Any]])](rdds.head.context) with Logging {

  override def splits = splits_

  override val partitioner = Some(part)

  override def preferredLocations(s: Split) = Nil

  override def compute(s: Split): Iterator[(K, Array[ArrayBuffer[Any]])] = {
    val split = s.asInstanceOf[CoGroupSplit]
    val numRdds = split.deps.size
    val map = new JHashMap[K, Array[ArrayBuffer[Any]]]
    def getSeq(k: K): Array[ArrayBuffer[Any]] = {
      var values = map.get(k)
      if (values == null) {
        values = Array.fill(numRdds)(new ArrayBuffer[Any])
        map.put(k, values)
      }
      values
    }
    val fetcher = SparkEnv.get.shuffleFetcher
    for ((dep, depNum) <- split.deps.zipWithIndex) dep match {
      case NarrowCoGroupSplitDep(rdd, itsSplit) => {
        // Read them from the parent
        for ((k, v) <- rdd.iterator(itsSplit)) {
          getSeq(k.asInstanceOf[K])(depNum) += v
        }
      }
      case ShuffleCoGroupSplitDep(shuffleId) => {
        fetcher.fetchMultiple[K, Any](shuffleId, split.partitions).foreach { case(k, v) =>
          getSeq(k)(depNum) += v
        }
      }
    }
    map.iterator
  }
}
