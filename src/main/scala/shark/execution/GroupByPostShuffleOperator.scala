package org.apache.hadoop.hive.ql.exec
// Put this file in Hive's exec package to access package level visible fields and methods.

import java.util.ArrayList
import java.util.{HashMap => JHashMap, HashSet => JHashSet, Set => JSet}

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.exec.{GroupByOperator => HiveGroupByOperator}
import org.apache.hadoop.hive.ql.plan.{ExprNodeColumnDesc, TableDesc}
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorUtils,
  StandardStructObjectInspector, StructObjectInspector, UnionObject}
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption
import org.apache.hadoop.hive.serde2.{Deserializer, SerDe}
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils
import org.apache.hadoop.io.BytesWritable

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import scala.reflect.BeanProperty

import shark.execution.{HiveTopOperator, ReduceKey, DependencyForcerRDD, CoalescedShuffleFetcherRDD,
  CoalescedShuffleSplit}
import spark.{Aggregator, HashPartitioner, RDD}
import spark.ShuffleDependency
import spark.SparkContext._
import spark.SparkEnv
import spark.rdd.ShuffledRDD



// The final phase of group by.
// TODO(rxin): For multiple distinct aggregations, use sort-based shuffle.
class GroupByPostShuffleOperator extends GroupByPreShuffleOperator
with HiveTopOperator {

  @BeanProperty var keyTableDesc: TableDesc = _
  @BeanProperty var valueTableDesc: TableDesc = _

  @transient var keySer: Deserializer = _
  @transient var valueSer: Deserializer = _
  @transient val distinctKeyAggrs = new JHashMap[Int, JSet[java.lang.Integer]]()
  @transient val nonDistinctKeyAggrs = new JHashMap[Int, JSet[java.lang.Integer]]()
  @transient val nonDistinctAggrs = new ArrayList[Int]()
  @transient val distinctKeyWrapperFactories = new JHashMap[Int, ArrayList[KeyWrapperFactory]]()
  @transient val distinctHashSets = new JHashMap[Int, ArrayList[JHashSet[KeyWrapper]]]()

  @transient var unionExprEvaluator: ExprNodeEvaluator = _

  override def initializeOnMaster() {
    super.initializeOnMaster()
    keyTableDesc = keyValueTableDescs.values.head._1
    valueTableDesc = keyValueTableDescs.values.head._2
  }

  override def initializeOnSlave() {

    super.initializeOnSlave()
    // Initialize unionExpr. KEY has union field as the last field if there are distinct aggrs.
    unionExprEvaluator = initializeUnionExprEvaluator(rowInspector)

    initializeKeyUnionAggregators()
    initializeKeyWrapperFactories()

    keySer = keyTableDesc.getDeserializerClass.newInstance().asInstanceOf[Deserializer]
    keySer.initialize(null, keyTableDesc.getProperties())

    valueSer = valueTableDesc.getDeserializerClass.newInstance().asInstanceOf[SerDe]
    valueSer.initialize(null, valueTableDesc.getProperties())
  }

  private def initializeKeyWrapperFactories() {
    distinctKeyAggrs.keySet.iterator.foreach { unionId =>
      val aggrIndices = distinctKeyAggrs.get(unionId)
      val evals = aggrIndices.map(i => aggregationParameterFields(i)).toArray
      val ois = aggrIndices.map(i => aggregationParameterObjectInspectors(i)).toArray
      val writableOis: Array[Array[ObjectInspector]] = ois.map { oi => oi.map { k =>
        ObjectInspectorUtils.getStandardObjectInspector(k, ObjectInspectorCopyOption.WRITABLE)
      }}.toArray

      val keys = new ArrayList[KeyWrapperFactory]()
      val hashSets = new ArrayList[JHashSet[KeyWrapper]]()
      for(i <- 0 until evals.size) {
        keys.add(new KeyWrapperFactory(evals(i), ois(i), writableOis(i)))
        hashSets.add(new JHashSet[KeyWrapper])
      }
      distinctHashSets.put(unionId, hashSets)
      distinctKeyWrapperFactories.put(unionId, keys)
    }
  }

  private def initializeUnionExprEvaluator(rowInspector: ObjectInspector): ExprNodeEvaluator = {
    val sfs = rowInspector.asInstanceOf[StructObjectInspector].getAllStructFieldRefs
    var unionExprEval: ExprNodeEvaluator = null
    if (sfs.size > 0) {
      val keyField = sfs.get(0)
      if (keyField.getFieldName.toUpperCase.equals(Utilities.ReduceField.KEY.name)) {
        val keyObjInspector = keyField.getFieldObjectInspector
        if (keyObjInspector.isInstanceOf[StandardStructObjectInspector]) {
          val keysfs = keyObjInspector.asInstanceOf[StructObjectInspector].getAllStructFieldRefs
          if (keysfs.size() > 0) {
            val sf = keysfs.get(keysfs.size() - 1)
            if (sf.getFieldObjectInspector().getCategory().equals(ObjectInspector.Category.UNION)) {
              unionExprEval = ExprNodeEvaluatorFactory.get(
                new ExprNodeColumnDesc(
                  TypeInfoUtils.getTypeInfoFromObjectInspector(sf.getFieldObjectInspector),
                  keyField.getFieldName + "." + sf.getFieldName, null, false
                )
              )
              unionExprEval.initialize(rowInspector)
            }
          }
        }
      }
    }
    unionExprEval
  }

  /**
   * This is used to initialize evaluators for distinct keys stored in
   * the union component of the key.
   */
  private def initializeKeyUnionAggregators() {
    val aggrs = conf.getAggregators
    for (i <- 0 until aggrs.size) {
      val aggr = aggrs.get(i)
      val parameters = aggr.getParameters
      for (j <- 0 until parameters.size) {
        if (unionExprEvaluator != null) {
          val names = parameters.get(j).getExprString().split("\\.")
          // parameters of the form : KEY.colx:t.coly
          if (Utilities.ReduceField.KEY.name().equals(names(0))) {
            val name = names(names.length - 2)
            val tag = Integer.parseInt(name.split("\\:")(1))
            if (aggr.getDistinct()) {
              var set = distinctKeyAggrs.get(tag)
              if (set == null) {
                set = new JHashSet[java.lang.Integer]()
                distinctKeyAggrs.put(tag, set)
              }
              if (!set.contains(i)) {
                set.add(i)
              }
            } else {
              var set = nonDistinctKeyAggrs.get(tag)
              if (set == null) {
                set = new JHashSet[java.lang.Integer]()
                nonDistinctKeyAggrs.put(tag, set)
              }
              if (!set.contains(i)) {
                set.add(i)
              }
            }
          } else {
            // will be VALUE._COLx
            if (!nonDistinctAggrs.contains(i)) {
              nonDistinctAggrs.add(i)
            }
          }
        }
      }
      if (parameters.size() == 0) {
        // for ex: count(*)
        if (!nonDistinctAggrs.contains(i)) {
          nonDistinctAggrs.add(i)
        }
      }
    }
  }

  override def preprocessRdd(rdd: RDD[_]): RDD[_] = {
    // We don't use Spark's groupByKey to avoid map-side combiners in Spark.
    val maxPartitions = 100  // TODO: make this configurable
    if (conf.getKeys.size == 0) {
      // If we have no keys, we need to perform a total aggregation using one reducer.
      val aggregator = new Aggregator[Any, Any, ArrayBuffer[Any]](
        GroupByAggregator.createCombiner _,
        GroupByAggregator.mergeValue _,
        GroupByAggregator.mergeCombiners _)

      return new ShuffledRDD(
        rdd.asInstanceOf[RDD[(Any, Any)]],
        new HashPartitioner(1)).mapPartitions(aggregator.combineValuesByKey)
    }
    // Perform fine-grained pre-partitioning, forcing the ShuffleDependency to be computed but
    // skipping the shuffle fetching phase.
    val NUM_FINE_GRAINED_BUCKETS = maxPartitions
    val part = new HashPartitioner(NUM_FINE_GRAINED_BUCKETS)
    val pairRdd = rdd.asInstanceOf[RDD[(Any, Any)]]
    val dep = new ShuffleDependency[Any, Any](pairRdd, part)
    val depForcer = new DependencyForcerRDD(pairRdd, List(dep))
    rdd.context.runJob(depForcer, (iter: Iterator[_]) => {})

    // Collect the partition sizes
    val mapOutputTracker = SparkEnv.get.mapOutputTracker
    val partitionSizes = 0.until(NUM_FINE_GRAINED_BUCKETS).map(
      mapOutputTracker.getServerStatuses(dep.shuffleId, _).map(_._2).sum)
    logInfo("Computed fine-grained shuffle partitions with sizes: " + partitionSizes)

    // Mke a partitioning decision based on statistics.
    // For now, we'll use a simple heuristic based on the total data set size,
    // which aims to keep the amount of data per partition above a static threshold.
    val MIN_BYTES_PER_PARTITION = 32 * 1024 * 1024 // 32 megabytes  TODO: make this configurable
    val totalDataSetSize = partitionSizes.sum
    logInfo("Total data set size is: " + totalDataSetSize)

    val numCoalescedPartitions = math.min(math.round(math.ceil(1.0 * totalDataSetSize / MIN_BYTES_PER_PARTITION)),
      maxPartitions).toInt
    logInfo("Coalescing " + NUM_FINE_GRAINED_BUCKETS + " fine-grained partitions into " +
      numCoalescedPartitions + " partitions")

    // Form $n$ sublists, assigning to them in round-robin fashion.
    // From http://stackoverflow.com/questions/11132788/
    def round[T](l: List[T], n: Int): List[List[T]] =
      (0 until n).map{ i => l.drop(i).sliding(1, n).flatten.toList }.toList

    val groups = round(0.until(NUM_FINE_GRAINED_BUCKETS).toList, numCoalescedPartitions)
    val groupedSplits = groups.zipWithIndex.map(x => new CoalescedShuffleSplit(x._2, x._1.toArray)).toArray

    // This RDD will fetch the coalesced partitions
    val coalesced = new CoalescedShuffleFetcherRDD(pairRdd, dep, groupedSplits)
    coalesced.mapPartitions(iter => {
      val combiners = new JHashMap[Any, ArrayBuffer[Any]]
      for ((k, v) <- iter) {
        val oldC = combiners.get(k)
        if (oldC == null) {
          combiners.put(k, GroupByAggregator.createCombiner(v))
        } else {
          combiners.put(k, GroupByAggregator.mergeValue(oldC, v))
        }
      }
      combiners.iterator
    })
  }

  override def processPartition(split: Int, iter: Iterator[_]) = {
    // TODO: we should support outputs besides BytesWritable in case a different
    // SerDe is used for intermediate data.
    val bytes = new BytesWritable()
    logInfo("Running Post Shuffle Group-By")
    val outputCache = new Array[Object](keyFields.length + aggregationEvals.length)

    // The reusedRow is used to conform to Hive's expected row format.
    // It is an array of [key, value] that is reused across rows
    val reusedRow = new Array[Any](2)

    val keys = keyFactory.getKeyWrapper()
    val aggrs = newAggregations()

    val newIter = iter.map { case (key: ReduceKey, values: Seq[_]) =>
      bytes.set(key.bytes)
      val deserializedKey = deserializeKey(bytes)
      reusedRow(0) = deserializedKey
      resetAggregations(aggrs)
      values.foreach {
        case v: Array[Byte] => {
          bytes.set(v)
          reusedRow(1) = deserializeValue(bytes)
          aggregate(reusedRow, aggrs, false)
        }
        case (key: Array[Byte], value: Array[Byte]) => {
          bytes.set(key)
          val deserializedUnionKey = deserializeKey(bytes)
          bytes.set(value)
          val deserializedValue = deserializeValue(bytes)
          val row = Array(deserializedUnionKey, deserializedValue)
          keys.getNewKey(row, rowInspector)
          val uo =  unionExprEvaluator.evaluate(row).asInstanceOf[UnionObject]
          val unionTag = uo.getTag().toInt
          // Handle non-distincts in the key-union
          if (nonDistinctKeyAggrs.get(unionTag) != null) {
            nonDistinctKeyAggrs.get(unionTag).foreach { i =>
              val o = aggregationParameterFields(i).map(_.evaluate(row)).toArray
              aggregationEvals(i).aggregate(aggrs(i), o)
            }
          }
          // Handle non-distincts in the value
          if (unionTag == 0) {
            nonDistinctAggrs.foreach { i =>
              val o = aggregationParameterFields(i).map(_.evaluate(row)).toArray
              aggregationEvals(i).aggregate(aggrs(i), o)
            }
          }
          // Handle distincts
          if (distinctKeyAggrs.get(unionTag) != null) {
            // This assumes that we traverse the aggr Params in the same order
            val aggrIndices = distinctKeyAggrs.get(unionTag).iterator
            val factories = distinctKeyWrapperFactories.get(unionTag)
            val hashes = distinctHashSets.get(unionTag)
            for (i <- 0 until factories.size) {
              val aggrIndex = aggrIndices.next
              val key: KeyWrapper = factories.get(i).getKeyWrapper()
              key.getNewKey(row, rowInspector)
              key.setHashKey()
              var seen = hashes(i).contains(key)
              if (!seen) {
                aggregationEvals(aggrIndex).aggregate(aggrs(aggrIndex), key.getKeyArray)
                hashes(i).add(key.copyKey())
              }
            }
          }
        }
      }

      // Reset hash sets for next group-by key
      distinctHashSets.values.foreach { hashSet => hashSet.foreach { _.clear() } }

      // Copy output keys and values to our reused output cache
      var i = 0
      var numKeys = keyFields.length
      while (i < numKeys) {
        outputCache(i) = keyFields(i).evaluate(reusedRow)
        i += 1
      }
      while (i < numKeys + aggrs.length) {
        outputCache(i) = aggregationEvals(i - numKeys).evaluate(aggrs(i - numKeys))
        i += 1
      }
      outputCache
    }

    if (!newIter.hasNext && keyFields.size == 0) {
      Iterator(createEmptyRow()) // We return null if there are no rows
    } else {
      newIter
    }
  }

  private def createEmptyRow(): Array[Object] = {
    val aggrs = newAggregations()
    val output = new Array[Object](aggrs.size)
    for (i <- 0 until aggrs.size) {
      var emptyObj: Array[Object] = null
      if (aggregationParameterFields(i).length > 0) {
        emptyObj = aggregationParameterFields.map { field => null }.toArray
      }
      aggregationEvals(i).aggregate(aggrs(i), emptyObj)
      output(i) = aggregationEvals(i).evaluate(aggrs(i))
    }
    output
  }

  private def deserializeKey(bytes: BytesWritable): Object = keySer.deserialize(bytes)

  private def deserializeValue(bytes: BytesWritable): Object = valueSer.deserialize(bytes)

  private def resetAggregations(aggs: Array[AggregationBuffer]) {
    var i = 0
    while (i < aggs.length) {
      aggregationEvals(i).reset(aggs(i))
      i += 1
    }
  }
}


object GroupByAggregator {
  def createCombiner(v: Any) = ArrayBuffer(v)
  def mergeValue(buf: ArrayBuffer[Any], v: Any) = buf += v
  def mergeCombiners(b1: ArrayBuffer[Any], b2: ArrayBuffer[Any]) = b1 ++= b2
}
