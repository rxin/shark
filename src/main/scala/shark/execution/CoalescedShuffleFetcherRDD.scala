package shark.execution

import spark.RDD
import spark.ShuffleDependency
import spark.SparkEnv
import spark.Split


class CoalescedShuffleSplit(
    val index: Int,
    val partitions: Array[Int])
  extends Split

class CoalescedShuffleFetcherRDD[K, V](
    prev: RDD[(K, V)],
    dep: ShuffleDependency[K, V],
    coalescedPartitions: Array[CoalescedShuffleSplit])
    extends RDD[(K, V)](prev.context) {

    override def splits = coalescedPartitions.asInstanceOf[Array[Split]]

    override val dependencies = List(dep)

    override def compute(split: Split) = {
      // TODO: fix
      val fetcher = SparkEnv.get.shuffleFetcher
      split.asInstanceOf[CoalescedShuffleSplit].partitions.flatMap(part => {
        fetcher.fetch[K, V](dep.shuffleId, part)
      }).iterator
    }

    override def preferredLocations(split: Split) = Nil
  }

