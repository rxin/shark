package shark.execution

import java.util.{HashMap => JHashMap, List => JList}

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.exec.{JoinOperator => HiveJoinOperator}
import org.apache.hadoop.hive.ql.plan.{JoinDesc, TableDesc}
import org.apache.hadoop.hive.serde2.{Deserializer, Serializer, SerDeUtils}
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector
import org.apache.hadoop.io.BytesWritable

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import scala.reflect.BeanProperty

import shark.LogHelper
import spark.SparkEnv
import spark.{HashPartitioner, RDD, Split}
import spark.ShuffleDependency
import spark.SparkContext._


class JoinOperator extends CommonJoinOperator[JoinDesc, HiveJoinOperator]
  with HiveTopOperator with LogHelper {

  @BeanProperty var valueTableDescMap: JHashMap[Int, TableDesc] = _
  @BeanProperty var keyTableDesc: TableDesc = _

  @transient var tagToValueSer: JHashMap[Int, Deserializer] = _
  @transient var keyDeserializer: Deserializer = _
  @transient var keyObjectInspector: StandardStructObjectInspector = _

  val NUM_FINE_GRAINED_BUCKETS = 100
  val SMALL_TABLE_SIZE = 32 * 1024 * 1024
  val MIN_BYTES_PER_PARTITION = 32 * 1024 * 1024

  override def initializeOnMaster() {
    super.initializeOnMaster()
    valueTableDescMap = new JHashMap[Int, TableDesc]
    valueTableDescMap ++= keyValueTableDescs.map { case(tag, kvdescs) => (tag, kvdescs._2) }
    keyTableDesc = keyValueTableDescs.head._2._1

    // Call initializeOnSlave to initialize the join filters, etc.
    initializeOnSlave()
  }

  override def initializeOnSlave() {
    super.initializeOnSlave()

    tagToValueSer = new JHashMap[Int, Deserializer]
    valueTableDescMap foreach { case(tag, tableDesc) =>
      logDebug("tableDescs (tag %d): %s".format(tag, tableDesc))

      val deserializer = tableDesc.getDeserializerClass.newInstance().asInstanceOf[Deserializer]
      deserializer.initialize(null, tableDesc.getProperties())

      logDebug("value deser (tag %d): %s".format(tag, deserializer))
      tagToValueSer.put(tag, deserializer)
    }

    if (nullCheck) {
      keyDeserializer = keyTableDesc.getDeserializerClass.newInstance.asInstanceOf[Deserializer]
      keyDeserializer.initialize(null, keyTableDesc.getProperties())
      keyObjectInspector =
        keyDeserializer.getObjectInspector().asInstanceOf[StandardStructObjectInspector]
    }
  }

  override def execute(): RDD[_] = {
    val inputRdds = executeParents()
    combineMultipleRdds(inputRdds)
  }

  def runPartialDag(rdd: RDD[_]): (ShuffleDependency[Any, Any, Any], IndexedSeq[Long]) = {
    val part = new HashPartitioner(NUM_FINE_GRAINED_BUCKETS)
    val pairRdd = rdd.asInstanceOf[RDD[(Any, Any)]]
    val dep = new ShuffleDependency[Any, Any, Any](pairRdd, None, part)
    val depForcer = new DependencyForcerRDD(pairRdd, List(dep))
    depForcer.forceEvaluate()

    // Collect the partition sizes
    val mapOutputTracker = SparkEnv.get.mapOutputTracker
    val partitionSizes = 0.until(NUM_FINE_GRAINED_BUCKETS).map(
      mapOutputTracker.getServerStatuses(dep.shuffleId, _).map(_._2).sum)
    logInfo("Computed fine-grained shuffle partitions with sizes: " + partitionSizes)

    (dep, partitionSizes)
  }

  override def combineMultipleRdds(rdds: Seq[(Int, RDD[_])]): RDD[_] = {
    // Determine the number of reduce tasks to run.
    var numReduceTasks = hconf.getIntVar(HiveConf.ConfVars.HADOOPNUMREDUCERS)
    if (numReduceTasks < 1) {
      numReduceTasks = 1
    }

    // Turn the RDD into a map. Use a Java HashMap to avoid Scala's annoying
    // Some/Option. Add an assert for sanity check. If ReduceSink's join tags
    // are wrong, the hash entries might collide.
    val rddsJavaMap = new JHashMap[Int, RDD[_]]
    rddsJavaMap ++= rdds
    assert(rdds.size == rddsJavaMap.size, {
      logError("rdds.size (%d) != rddsJavaMap.size (%d)".format(rdds.size, rddsJavaMap.size))
    })

    val rddsInJoinOrder: Seq[RDD[(ReduceKey, Any)]] = order.map { inputIndex =>
      rddsJavaMap.get(inputIndex.byteValue.toInt).asInstanceOf[RDD[(ReduceKey, Any)]]
    }.toSeq

    // Force execute the DAG before the join.
    val rddRuns = rddsInJoinOrder.map(runPartialDag)
    println("-------------------------------------------------------------")
    println(rddRuns)

    // Size of the join inputs.
    val inputSizes = rddRuns.map { rddRun => rddRun._2.sum }
    println(inputSizes)

    val smallTables = inputSizes.zipWithIndex.filter(_._1 < SMALL_TABLE_SIZE)
    if (smallTables.size >= numTables - 1) {
      // TODO: There are enough small tables. Use broadcast join.
      cogroupJoin(rddsInJoinOrder, rddRuns)
    } else {
      cogroupJoin(rddsInJoinOrder, rddRuns)
    }
  }

  def cogroupJoin(
    rddsInJoinOrder: Seq[RDD[(ReduceKey, Any)]],
    rddRuns: Seq[(ShuffleDependency[Any, Any, Any], IndexedSeq[Long])]): RDD[_] = {

    // Use normal shuffle join.
    val inputSizes = rddRuns.map { rddRun => rddRun._2.sum }
    val totalDataSetSize = inputSizes.sum
    val numCoalescedPartitions = math.min(
      math.round(math.ceil(1.0 * totalDataSetSize / MIN_BYTES_PER_PARTITION)),
      NUM_FINE_GRAINED_BUCKETS).toInt

    // Form $n$ sublists, assigning to them in round-robin fashion.
    // From http://stackoverflow.com/questions/11132788/
    def round[T](l: List[T], n: Int): List[List[T]] =
      (0 until n).map{ i => l.drop(i).sliding(1, n).flatten.toList }.toList

    val deps: Seq[ShuffleDependency[Any, Any, Any]] = rddRuns.map(_._1)
    val cogroupDeps: Seq[CoGroupSplitDep] = deps.map(d => new ShuffleCoGroupSplitDep(d.shuffleId))

    val groups = round(0.until(NUM_FINE_GRAINED_BUCKETS).toList, numCoalescedPartitions)
    val groupedSplits = groups.zipWithIndex.map { case(group, index) =>
      new CoGroupSplit(index, cogroupDeps, group.toArray) : Split
    }.toArray

    println("----------------------------------------------------------------")
    println("numCoalescedPartitions: " + numCoalescedPartitions)
    println("num groups: " + groups.size)

    val part = new HashPartitioner(numCoalescedPartitions)
    val cogrouped = new CoGroupedRDD[ReduceKey](
      rddsInJoinOrder.asInstanceOf[Seq[RDD[(_, _)]]], part, groupedSplits, deps.toList)

    val op = OperatorSerializationWrapper(this)

    cogrouped.mapPartitions { part =>
      op.initializeOnSlave()

      val tmp = new Array[Object](2)
      val writable = new BytesWritable
      val nullSafes = op.conf.getNullSafes()

      val cp = new CartesianProduct[Any](op.numTables)

      part.flatMap { case (k: ReduceKey, bufs: Array[_]) =>
        writable.set(k.bytes)

        // If nullCheck is false, we can skip deserializing the key.
        if (op.nullCheck &&
            SerDeUtils.hasAnyNullObject(
              op.keyDeserializer.deserialize(writable).asInstanceOf[JList[_]],
              op.keyObjectInspector,
              nullSafes)) {
          bufs.zipWithIndex.flatMap { case (buf, label) =>
            val bufsNull = Array.fill(op.numTables)(ArrayBuffer[Any]())
            bufsNull(label) = buf
            op.generateTuples(cp.product(bufsNull.asInstanceOf[Array[Seq[Any]]], op.joinConditions))
          }
        } else {
          op.generateTuples(cp.product(bufs.asInstanceOf[Array[Seq[Any]]], op.joinConditions))
        }
      }
    }
  }

  def generateTuples(iter: Iterator[Array[Any]]): Iterator[_] = {
    val tupleOrder = CommonJoinOperator.computeTupleOrder(joinConditions)

    val bytes = new BytesWritable()
    val tmp = new Array[Object](2)

    val tupleSizes = (0 until joinVals.size).map { i => joinVals.get(i.toByte).size() }.toIndexedSeq
    val offsets = tupleSizes.scanLeft(0)(_ + _)

    val rowSize = offsets.last
    val outputRow = new Array[Object](rowSize)

    iter.map { elements: Array[Any] =>
      var index = 0
      while (index < numTables) {
        val element = elements(index).asInstanceOf[Array[Byte]]
        var i = 0
        if (element == null) {
          while (i < joinVals.get(index.toByte).size) {
            outputRow(i + offsets(index)) = null
            i += 1
          }
        } else {
          bytes.set(element, 0, element.length)
          tmp(1) = tagToValueSer.get(index).deserialize(bytes)
          val joinVal = joinVals.get(index.toByte)
          while (i < joinVal.size) {
            outputRow(i + offsets(index)) = joinVal(i).evaluate(tmp)
            i += 1
          }
        }
        index += 1
      }

      outputRow
    }
  }

  override def processPartition(split: Int, iter: Iterator[_]): Iterator[_] =
    throw new UnsupportedOperationException("JoinOperator.processPartition()")
}
