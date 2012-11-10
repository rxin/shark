package shark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf


object SharkConfVars {

  val EXEC_MODE = new ConfVar("shark.exec.mode", "shark")

  // This is created for testing. Hive's test script assumes a certain output
  // format. To pass the test scripts, we need to use Hive's EXPLAIN.
  val EXPLAIN_MODE = new ConfVar("shark.explain.mode", "shark")

  // If true, keys that are NULL are equal. For strict SQL standard, set this to true.
  val JOIN_CHECK_NULL = new ConfVar("shark.join.checknull", true)

  // Specify the initial capacity for ArrayLists used to represent columns in columnar
  // cache. The default -1 for non-local mode means that Shark will try to estimate
  // the number of rows by using: partition_size / (num_columns * avg_field_size).
  val COLUMN_INITIALSIZE = new ConfVar("shark.columnar.cache.initialSize",
    if (System.getenv("MASTER") == null) 100 else -1)

  // If true, then cache any table whose name ends in "_cached".
  val CHECK_TABLENAME_FLAG = new ConfVar("shark.cache.flag.checkTableName", true)

  // Prune map splits for cached tables based on predicates in queries.
  val MAP_PRUNING = new ConfVar("shark.mappruning", true)

  // Print debug information for map pruning.
  val MAP_PRUNING_PRINT_DEBUG = new ConfVar("shark.mappruning.debug", false)

  // If true, then query plans are compressed before being sent
  val COMPRESS_QUERY_PLAN = new ConfVar("shark.compressQueryPlan", true)

  // If true, partial DAG execution is used during the groupBy phase.
  // If false, the number of reducers is controlled by 'mapred.reduce.tasks'
  val GROUP_BY_USE_PARTIAL_DAG = new ConfVar("shark.groupBy.usePartialDag", true)

  // Choose the heuristic used to choose the degree of parallelism in groupBy when using
  // partial DAG mode.  Valid settings are 'bytesPerReducer' and 'fixedNumber'.
  // Under 'fixedNumber' mode, the number of reducers is set by 'mapred.reduce.tasks'
  val GROUP_BY_PARALLELISM_HEURISTIC = new ConfVar("shark.groupBy.parallelismHeuristic", "bytesPerReducer")

  // The minimum number of bytes per reduce partition, used to control the degree
  // of parallelism in groupBy's partial DAG shuffle phase.
  val GROUP_BY_MIN_BYTES_PER_REDUCER = new ConfVar("shark.groupBy.minBytesPerReducer", 32 * 1024 * 1024)

  // The number of fine-grained buckets to use during groupBy's
  // partial DAG hash partitioning phase.
  val GROUP_BY_NUM_FINE_GRAINED_BUCKETS = new ConfVar("shark.groupBy.numFineGrainedBuckets", 1024)

  // If true, fine-grained group by partitions will be bin-packed using a heuristic
  // (only applies when shark.groupBy.usePartialDag is enabled.
  val GROUP_BY_USE_BIN_PACKING = new ConfVar("shark.groupBy.useBinPacking", true)

  // Use reflection to discover all ConfVar fields defined in this class
  private def allConfVars: Array[ConfVar] = {
    val confVarFields = SharkConfVars.getClass.getDeclaredFields.filter(_.getType() == classOf[ConfVar])
    confVarFields.map(field => {
      val isAccessible = field.isAccessible
      field.setAccessible(true)
      val value = field.get(this).asInstanceOf[ConfVar]
      field.setAccessible(isAccessible)
      value
    })
  }

  val NUM_COPARTITIONS = new ConfVar("shark.num.copartitions", 5)

  // Add Shark configuration variables and their default values to the given conf,
  // so default values show up in 'set'.
  def initializeWithDefaults(conf: Configuration) {
    for (confVar <- allConfVars) {
      if (conf.get(confVar.varname) == null) {
        ClassManifest.fromClass(confVar.valClass) match {
          case c if c == classManifest[String] => conf.set(confVar.varname, confVar.defaultVal)
          case c if c == ClassManifest.Int     => conf.setInt(confVar.varname, confVar.defaultIntVal)
          case c if c == ClassManifest.Boolean => conf.setBoolean(confVar.varname, confVar.defaultBoolVal)
          case c if c == ClassManifest.Long    => conf.setLong(confVar.varname, confVar.defaultLongVal)
          case c if c == ClassManifest.Float   => conf.setFloat(confVar.varname, confVar.defaultFloatVal)
          case _ => new IllegalStateException("Unexpected ConfVar valClass: " + confVar.valClass)
        }
      }
    }
  }

  def getIntVar(conf: Configuration, variable: ConfVar): Int = {
    require(variable.valClass == classOf[Int])
    conf.getInt(variable.varname, variable.defaultIntVal)
  }

  def getLongVar(conf: Configuration, variable: ConfVar): Long = {
    require(variable.valClass == classOf[Long])
    conf.getLong(variable.varname, variable.defaultLongVal)
  }

  def getFloatVar(conf: Configuration, variable: ConfVar): Float = {
    require(variable.valClass == classOf[Float])
    conf.getFloat(variable.varname, variable.defaultFloatVal)
  }

  def getBoolVar(conf: Configuration, variable: ConfVar): Boolean = {
    require(variable.valClass == classOf[Boolean])
    conf.getBoolean(variable.varname, variable.defaultBoolVal)
  }

  def getVar(conf: Configuration, variable: ConfVar): String = {
    require(variable.valClass == classOf[String])
    conf.get(variable.varname, variable.defaultVal)
  }

  def setVar(conf: Configuration, variable: ConfVar, value: String) {
    require(variable.valClass == classOf[String])
    conf.set(variable.varname, value)
  }

  def getIntVar(conf: Configuration, variable: HiveConf.ConfVars)
    = HiveConf.getIntVar _
  def getLongVar(conf: Configuration, variable: HiveConf.ConfVars)
    = HiveConf.getLongVar(conf, variable)
  def getLongVar(conf: Configuration, variable: HiveConf.ConfVars, defaultVal: Long)
    = HiveConf.getLongVar(conf, variable, defaultVal)
  def getFloatVar(conf: Configuration, variable: HiveConf.ConfVars)
    = HiveConf.getFloatVar(conf, variable)
  def getFloatVar(conf: Configuration, variable: HiveConf.ConfVars, defaultVal: Float)
    = HiveConf.getFloatVar(conf, variable, defaultVal)
  def getBoolVar(conf: Configuration, variable: HiveConf.ConfVars)
    = HiveConf.getBoolVar(conf, variable)
  def getBoolVar(conf: Configuration, variable: HiveConf.ConfVars, defaultVal: Boolean)
    = HiveConf.getBoolVar(conf, variable, defaultVal)
  def getVar(conf: Configuration, variable: HiveConf.ConfVars)
    = HiveConf.getVar(conf, variable)
  def getVar(conf: Configuration, variable: HiveConf.ConfVars, defaultVal: String)
    = HiveConf.getVar(conf, variable, defaultVal)

}


case class ConfVar(
  varname: String,
  valClass: Class[_],
  defaultVal: String,
  defaultIntVal: Int,
  defaultLongVal: Long,
  defaultFloatVal: Float,
  defaultBoolVal: Boolean) {

  def this(varname: String, defaultVal: String) = {
    this(varname, classOf[String], defaultVal, 0, 0, 0, false)
  }

  def this(varname: String, defaultVal: Int) = {
    this(varname, classOf[Int], null, defaultVal, 0, 0, false)
  }

  def this(varname: String, defaultVal: Long) = {
    this(varname, classOf[Long], null, 0, defaultVal, 0, false)
  }

  def this(varname: String, defaultVal: Float) = {
    this(varname, classOf[Float], null, 0, 0, defaultVal, false)
  }

  def this(varname: String, defaultVal: Boolean) = {
    this(varname, classOf[Boolean], null, 0, 0, 0, defaultVal)
  }
}
