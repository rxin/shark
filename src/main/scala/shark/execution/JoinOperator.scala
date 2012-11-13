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

import spark.{CoGroupedRDD, HashPartitioner, RDD}
import spark.SparkContext._
import spark.SparkEnv
import shark.{SharkConfVars, SharkEnv, Utils}
import spark.ShuffleDependency
import spark.SparkEnv
import spark.scheduler.ShuffleBlockStatus
import spark.CoGroupSplitDep
import spark.ShuffleCoGroupSplitDep
import spark.Split
import spark.CoGroupSplit
import spark.broadcast.Broadcast
import spark.PreshuffleResult


class JoinOperator extends CommonJoinOperator[JoinDesc, HiveJoinOperator]
  with HiveTopOperator {

  @BeanProperty var valueTableDescMap: JHashMap[Int, TableDesc] = _
  @BeanProperty var keyTableDesc: TableDesc = _

  @transient var tagToValueSer: JHashMap[Int, Deserializer] = _
  @transient var keyDeserializer: Deserializer = _
  @transient var keyObjectInspector: StandardStructObjectInspector = _

  // hconf is not initialized by the time we reach this part of the constructor,
  // so the 'lazy' keyword avoids a NullPointerException
  lazy val NUM_REDUCERS = math.max(hconf.getIntVar(HiveConf.ConfVars.HADOOPNUMREDUCERS), 1)

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

  override def combineMultipleRdds(rdds: Seq[(Int, RDD[_])]): RDD[_] = {
    // Turn the RDD into a map. Use a Java HashMap to avoid Scala's annoying
    // Some/Option. Add an assert for sanity check. If ReduceSink's join tags
    // are wrong, the hash entries might collide.
    val rddsJavaMap = new JHashMap[Int, RDD[_]]
    rddsJavaMap ++= rdds
    assert(rdds.size == rddsJavaMap.size, {
      logError("rdds.size (%d) != rddsJavaMap.size (%d)".format(rdds.size, rddsJavaMap.size))
    })

    println("partitioners:")
    println(rdds.map(_._2.partitioner))
    println(rdds.map(_._2))

    val rdd1 = rdds(0)._2.asInstanceOf[RDD[(_, _)]]
    val rdd2 = rdds(1)._2.asInstanceOf[RDD[(_, _)]]
    val part1 = rdd1.partitioner
    val part2 = rdd2.partitioner
    // val rdd1locs = SharkEnv.getCacheLocs(rdd1)
    // val rdd2locs = SharkEnv.getCacheLocs(rdd2)

    // rdd1locs zip rdd2locs map { case(split1, split2) =>
    //   logInfo(split1 + " " + split2)
    //   assert(split1 == split2)
    // }

    if (part1 == part2 && part1 != None) {

      import spark.OneToOneDependency

      rdd1.splits.zipWithIndex.foreach { case (split, index) =>
        println("split #" + index + ": " + SharkEnv.getPreferredLocs(rdd1, index))
      }

      rdd2.splits.zipWithIndex.foreach { case (split, index) =>
        println("split #" + index + ": " + SharkEnv.getPreferredLocs(rdd2, index))
      }

      val cogrouped = new CoGroupedRDD[ReduceKey](Seq(rdd1, rdd2), part1.get,
        List(new OneToOneDependency(rdd1), new OneToOneDependency(rdd2)))

      logInfo("cogroup splits : " + cogrouped.splits.toSeq)

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

    } else {

      val rddsInJoinOrder = order.map { inputIndex =>
        rddsJavaMap.get(inputIndex.byteValue.toInt).asInstanceOf[RDD[(ReduceKey, Any)]]
      }

      // Force execute the DAG before the join.
      val rddRuns = rddsInJoinOrder.map(_.preshuffle(new HashPartitioner(NUM_REDUCERS)))

      // Size of the join inputs.
      val inputSizes = rddRuns.map(_.sizes.sum)
      logInfo("Input table sizes: " + inputSizes.toSeq)

      val autoConvertMapJoin: Boolean = hconf.getBoolVar(HiveConf.ConfVars.HIVECONVERTJOIN)

      val smallTableSize: Long = hconf.getLongVar(HiveConf.ConfVars.HIVESMALLTABLESFILESIZE)
      val smallTables = inputSizes.zipWithIndex.filter(_._1 < smallTableSize)
      logInfo("Converting to map join if table is smaller than " +
        Utils.memoryBytesToString(smallTableSize))

      if (autoConvertMapJoin && smallTables.size >= numTables - 1) {
        val bigTableIndex = inputSizes.zipWithIndex.max._2
        broadcastJoin(rddsInJoinOrder, rddRuns, bigTableIndex)
      } else {
        cogroupJoin(rddsInJoinOrder, rddRuns)
      }

    }
  }

  def broadcastJoin(
    rddsInJoinOrder: Seq[RDD[(ReduceKey, Any)]],
    rddRuns: Seq[PreshuffleResult[_, _, _, _]],
    bigTableIndex: Int): RDD[_] = {

    logInfo("Executing broadcast join (auto converted).")

    // Collect the small tables.
    val fetcher = SparkEnv.get.shuffleFetcher
    val broadcastedTables: Seq[(Broadcast[ArrayBuffer[(ReduceKey, Any)]], Int)] =
      rddRuns.zipWithIndex.filter(_._2 != bigTableIndex).map { case(rddRun, index) =>
        val dep = rddRun.dep
        val shuffleId = dep.shuffleId
        val table = new ArrayBuffer[(ReduceKey, Any)]

        val startTime = System.currentTimeMillis()
        fetcher.fetchMultiple[ReduceKey, Any](
          shuffleId, Array.range(0, NUM_REDUCERS)).foreach { pair =>
          table += pair
        }
        val endTime = System.currentTimeMillis()

        logInfo("Fetching table (size: %d) took %d ms".format(
          rddRun.sizes.sum, endTime - startTime))

        (SharkEnv.sc.broadcast(table), index)
      }

    val bigTableRdd = new CoalescedBlockRDD[(ReduceKey, Any)](
      rddsInJoinOrder(bigTableIndex).context,
      rddRuns(bigTableIndex).dep.shuffleId)

    val op = OperatorSerializationWrapper(this)
    val numRdds = numTables

    bigTableRdd.mapPartitions { part =>

      val map = new JHashMap[ReduceKey, Array[ArrayBuffer[Any]]]
      def getSeq(k: ReduceKey): Array[ArrayBuffer[Any]] = {
        var values = map.get(k)
        if (values == null) {
          values = Array.fill(numRdds)(new ArrayBuffer[Any])
          map.put(k, values)
        }
        values
      }

      broadcastedTables.foreach { case(table, tableIndex) =>
        table.value.foreach { case(key, value) =>
          getSeq(key)(tableIndex) += value
        }
      }

      val cp = new CartesianProduct[Any](op.numTables)
      op.initializeOnSlave()

      val tmp = new Array[Object](2)
      val writable = new BytesWritable
      val nullSafes = op.conf.getNullSafes()
      val largeTableBuffer = new ArrayBuffer[Any](1)
      largeTableBuffer += null

      part.flatMap { case(key: ReduceKey, value: Any) =>

        val bufs = map.get(key)

        if (bufs == null) {
          Iterator.empty
        } else {
          largeTableBuffer(0) = value
          bufs(bigTableIndex) = largeTableBuffer

          writable.set(key.bytes)

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
  }

  def cogroupJoin(rddsInJoinOrder: Seq[RDD[(ReduceKey, Any)]], rddRuns: Seq[PreshuffleResult[_, _, _, _]]): RDD[_] = {

    logInfo("Executing shuffle join")

    val deps = rddRuns.map(_.dep).toList
    val part = new HashPartitioner(NUM_REDUCERS)
    val cogrouped = new CoGroupedRDD[ReduceKey](rddsInJoinOrder.asInstanceOf[Seq[RDD[(_, _)]]], part, deps)


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
