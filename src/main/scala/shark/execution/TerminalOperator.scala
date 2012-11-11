package shark.execution

import java.util.Date

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.exec.{FileSinkOperator => HiveFileSinkOperator, JobCloseFeedBack}
import org.apache.hadoop.hive.serde2.Serializer
import org.apache.hadoop.hive.serde2.SerDe
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector
import org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe
import org.apache.hadoop.mapred.{TaskID, TaskAttemptID, HadoopWriter}

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.reflect.BeanProperty

import shark.{RDDUtils, SharkConfVars, SharkEnv, Utils}
import shark.memstore._
import spark.{GrowableAccumulableParam, RDD, TaskContext}
import spark.EnhancedRDD._
import spark.SparkContext._
import spark.HashPartitioner
import spark.rdd.ShuffledRDD
import spark.SparkEnv


/**
 * File sink operator. It can accomplish one of the three things:
 * - write query output to disk
 * - cache query output
 * - return query as RDD directly (without materializing it)
 */
class TerminalOperator extends UnaryOperator[HiveFileSinkOperator] {

  // Create a local copy of hconf and hiveSinkOp so we can XML serialize it.
  @BeanProperty var localHiveOp: HiveFileSinkOperator = _
  @BeanProperty var localHconf: HiveConf = _
  @BeanProperty val now = new Date()

  override def initializeOnMaster() {
    localHconf = super.hconf
    // Set parent to null so we won't serialize the entire query plan.
    hiveOp.setParentOperators(null)
    hiveOp.setChildOperators(null)
    hiveOp.setInputObjInspectors(null)
    localHiveOp = hiveOp
  }

  override def initializeOnSlave() {
    localHiveOp.initialize(localHconf, Array(objectInspector))
  }

  override def processPartition(split: Int, iter: Iterator[_]): Iterator[_] = iter
}


class FileSinkOperator extends TerminalOperator with Serializable {

  // Pass the file extension ConfVar used by HiveFileSinkOperator.
  @BeanProperty var outputFileExtension: String = _

  override def initializeOnMaster() {
    super.initializeOnMaster()
    outputFileExtension = HiveConf.getVar(localHconf, HiveConf.ConfVars.OUTPUT_FILE_EXTENSION)
  }

  def initializeOnSlave(context: TaskContext) {
    setConfParams(localHconf, context)
    initializeOnSlave()
  }

  def setConfParams(conf: HiveConf, context: TaskContext) {
    val jobID = context.stageId
    val splitID = context.splitId
    val jID = HadoopWriter.createJobID(now, jobID)
    val taID = new TaskAttemptID(new TaskID(jID, true, splitID), 0)
    conf.set("mapred.job.id", jID.toString)
    conf.set("mapred.tip.id", taID.getTaskID.toString)
    conf.set("mapred.task.id", taID.toString)
    conf.setBoolean("mapred.task.is.map", true)
    conf.setInt("mapred.task.partition", splitID)

    // Variables used by FileSinkOperator.
    if (outputFileExtension != null) {
      conf.setVar(HiveConf.ConfVars.OUTPUT_FILE_EXTENSION, outputFileExtension)
    }
  }

  override def processPartition(split: Int, iter: Iterator[_]): Iterator[_] = {
    iter.foreach { row =>
      localHiveOp.processOp(row, 0)
    }

    // Create missing parent directories so that the HiveFileSinkOperator can rename
    // temp file without complaining.

    // Two rounds of reflection are needed, since the FSPaths reference is private, and
    // the FSPaths' finalPaths reference isn't publicly accessible.
    val fspField = localHiveOp.getClass.getDeclaredField("fsp")
    fspField.setAccessible(true)
    val fileSystemPaths = fspField.get(localHiveOp).asInstanceOf[HiveFileSinkOperator#FSPaths]

    // File paths for dynamic partitioning are determined separately. See FileSinkOperator.java.
    if (fileSystemPaths != null) {
      val finalPathsField = fileSystemPaths.getClass.getDeclaredField("finalPaths")
      finalPathsField.setAccessible(true)
      val finalPaths = finalPathsField.get(fileSystemPaths).asInstanceOf[Array[Path]]

      // Get a reference to the FileSystem. No need for reflection here.
      val fileSystem = FileSystem.get(localHconf)

      for (idx <- 0 until finalPaths.length) {
        var finalPath = finalPaths(idx)
        if (finalPath == null) {
          // If a query results in no output rows, then file paths for renaming will be
          // created in localHiveOp.closeOp instead of processOp. But we need them before
          // that to check for missing parent directories.
          val createFilesMethod = localHiveOp.getClass.getDeclaredMethod(
            "createBucketFiles", classOf[HiveFileSinkOperator#FSPaths])
          createFilesMethod.setAccessible(true)
          createFilesMethod.invoke(localHiveOp, fileSystemPaths)
          finalPath = finalPaths(idx)
        }
        if (!fileSystem.exists(finalPath.getParent())) fileSystem.mkdirs(finalPath.getParent())
      }
    }

    localHiveOp.closeOp(false)
    iter
  }

  override def execute(): RDD[_] = {
    val inputRdd = if (parentOperators.size == 1) executeParents().head._2 else null
    val rddPreprocessed = preprocessRdd(inputRdd)
    rddPreprocessed.context.runJob(
      rddPreprocessed, FileSinkOperator.executeProcessFileSinkPartition(this))
    hiveOp.jobClose(localHconf, true, new JobCloseFeedBack)
    rddPreprocessed
  }
}


object FileSinkOperator {
  def executeProcessFileSinkPartition(operator: FileSinkOperator) = {
    val op = OperatorSerializationWrapper(operator)
    def writeFiles(context: TaskContext, iter: Iterator[_]): Boolean = {
      op.logDebug("Started executing mapPartitions for operator: " + op)
      op.logDebug("Input object inspectors: " + op.objectInspectors)

      op.initializeOnSlave(context)
      val newPart = op.processPartition(-1, iter)
      op.logDebug("Finished executing mapPartitions for operator: " + op)

      true
    }
    writeFiles _
  }
}


/**
 * Cache the RDD and force evaluate it (so the cache is filled).
 */
class CacheSinkOperator(@BeanProperty var tableName: String)
  extends TerminalOperator {

  @BeanProperty var initialColumnSize: Int = _
  @BeanProperty var coPartitionTableName: String = _
  @BeanProperty var partitionColName: String = _

  // Zero-arg constructor for deserialization.
  def this() = this(null)

  override def initializeOnMaster() {
    super.initializeOnMaster()
    initialColumnSize = SharkConfVars.getIntVar(localHconf, SharkConfVars.COLUMN_INITIALSIZE)
  }

  override def initializeOnSlave() {
    super.initializeOnSlave()
    localHconf.setInt(SharkConfVars.COLUMN_INITIALSIZE.varname, initialColumnSize)
  }

  override def execute(): RDD[_] = {
    val inputRdd = if (parentOperators.size == 1) executeParents().head._2 else null

    val statsAcc = SharkEnv.sc.accumulableCollection(ArrayBuffer[(Int, TableStats)]())
    val op = OperatorSerializationWrapper(this)
    val shouldPartition = partitionColName != ""

    // partition here
    val rddToCache = {
      if (shouldPartition) {
        // Check if we should CoPartition this table according to another table
        val (partitioner, locations) =
          SharkEnv.cache.get(CacheKey(coPartitionTableName)) match {
            case Some(coRdd) => {
              coRdd.partitioner match {
                case Some(partitioner) => (partitioner, SharkEnv.getCacheLocs(coRdd))
                case _ => {
                  op.logInfo("no partitioner found in existing Rdd")
                  (new HashPartitioner(SharkConfVars.getIntVar(localHconf,
                     SharkConfVars.NUM_COPARTITIONS)), null)
                }
              }
            }
            case _ => {
              (new HashPartitioner(SharkConfVars.getIntVar(localHconf,
                 SharkConfVars.NUM_COPARTITIONS)), null)
            }
          }

        logInfo("Partitioning %s using partitioner %s on column %s".format(
          tableName, partitioner, partitionColName))

        // Need to create K,V form of the rdd we are caching
        val kvRdd = inputRdd.mapPartitions { rowIter =>
          op.initializeOnSlave()

          val sois = op.objectInspector.asInstanceOf[StructObjectInspector]
          val structField = sois.getStructFieldRef(op.partitionColName)

          val serde = new BinarySortableSerDe()
          shark.SharkEnvSlave.objectInspectorLock.synchronized {
            serde.initialize(op.hconf, op.localHiveOp.getConf.getTableInfo.getProperties)
          }

          rowIter.map { row =>
            val k = sois.getStructFieldData(row, structField)
            val value = serde.serialize(row, op.objectInspector).asInstanceOf[BytesWritable]
            val valueArr = new Array[Byte](value.getLength)

            // TODO(rxin): we don't need to copy bytes if we get rid of Spark block
            // manager's double buffering.
            Array.copy(value.getBytes, 0, valueArr, 0, value.getLength)
            (k, valueArr)
          }
        }

        kvRdd.partitionByWithLocations(partitioner, false, locations).mapPartitions(
          {iter => iter.map(x => x._2)}, true)
      } else {
        inputRdd
      }
    }

    logInfo("rddToCache's partitioner is " + rddToCache.partitioner)

    // Serialize the RDD on all partitions before putting it into the cache.
    val rdd = rddToCache.mapPartitionsWithSplit({ case(split, iter) =>
      op.initializeOnSlave()

      val serdeClass = op.localHiveOp.getConf.getTableInfo.getDeserializerClass
      op.logInfo("Using serde: " + serdeClass)
      val serde = serdeClass.newInstance().asInstanceOf[ColumnarSerDe]
      serde.initialize(op.hconf, op.localHiveOp.getConf.getTableInfo.getProperties())
      val rddSerialzier = new RDDSerializer(serde)

      val (iterToUse, ois) = if (shouldPartition) {
        val serdeDeserialize = new BinarySortableSerDe()
        shark.SharkEnvSlave.objectInspectorLock.synchronized {
          serdeDeserialize.initialize(op.hconf, op.localHiveOp.getConf.getTableInfo.getProperties)
        }
        val bw = new BytesWritable()
        val mappedIter = iter.map { x =>
          bw.set(x.asInstanceOf[Array[Byte]], 0, x.asInstanceOf[Array[Byte]].length)
          serdeDeserialize.deserialize(bw)
        }
        (mappedIter, serdeDeserialize.getObjectInspector)
      } else {
        (iter, op.objectInspector)
      }

      val iterToReturn = rddSerialzier.serialize(iterToUse, ois)

      statsAcc += (split, serde.stats)
      iterToReturn
    }, true)

    // Put the RDD in cache and force evaluate it.
    val cacheKey = new CacheKey(tableName)
    SharkEnv.cache.put(cacheKey, rdd)
    rdd.foreach(_ => Unit)

    // Report remaining memory.
    /* Commented out for now waiting for the reporting code to make into Spark.
    val remainingMems: Map[String, (Long, Long)] = SharkEnv.sc.getSlavesMemoryStatus
    remainingMems.foreach { case(slave, mem) =>
      println("%s: %s / %s".format(
        slave,
        Utils.memoryBytesToString(mem._2),
        Utils.memoryBytesToString(mem._1)))
    }
    println("Summary: %s / %s".format(
      Utils.memoryBytesToString(remainingMems.map(_._2._2).sum),
      Utils.memoryBytesToString(remainingMems.map(_._2._1).sum)))
    */

    // Get the column statistics back to the cache manager.
    SharkEnv.cache.keyToStats.put(cacheKey, statsAcc.value.toMap)

    if (SharkConfVars.getBoolVar(localHconf, SharkConfVars.MAP_PRUNING_PRINT_DEBUG)) {
      statsAcc.value.foreach { case(split, tableStats) =>
        println("Split " + split)
        println(tableStats.toString)
      }
    }

    // Return the cached RDD.
    rdd
  }

  override def processPartition(split: Int, iter: Iterator[_]): Iterator[_] =
    throw new UnsupportedOperationException("CacheSinkOperator.processPartition()")
}


/**
 * Collect the output as a TableRDD.
 */
class TableRddSinkOperator extends TerminalOperator {}
