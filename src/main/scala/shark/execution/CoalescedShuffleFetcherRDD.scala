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
    groups: Array[Array[Int]],
    dep: ShuffleDependency[K, V])
  extends RDD[(K, V)](prev.context) {

    override def splits = groups.zipWithIndex.map { case (group, index) =>
      new CoalescedShuffleSplit(index, group)
    }

    override val dependencies = List(dep)

    override def compute(split: Split) = {
      val fetcher = SparkEnv.get.shuffleFetcher
      val reduceIds = split.asInstanceOf[CoalescedShuffleSplit].partitions
      fetcher.fetchMultiple(dep.shuffleId, reduceIds)
    }

    override def preferredLocations(split: Split) = Nil
  }

