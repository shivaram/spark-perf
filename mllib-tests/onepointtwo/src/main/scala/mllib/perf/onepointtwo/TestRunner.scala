package mllib.perf.onepointtwo

import scala.collection.JavaConverters._

import sys.process._

import org.json4s.JsonDSL._
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object TestRunner {

  /**
   * Best-efforts to warm up network links between all worker pairs.
   *
   * Useful for making fullJob as similar to minJob as possible.  In minJob, # machines
   * is usually small, therefore the warmup trial(s) usually warm up all links.  But this
   * might not be the case in fullJob which has a much larger number of machines.
   */
  def warmUpNetworkLinks(sc: SparkContext): Unit = {
    import org.apache.spark.SparkContext._
    val numExec = sc.getExecutorMemoryStatus.size
    def rdd = sc.parallelize(1 to numExec * 2, numExec * 2).setName("WarmUpNetworkRDD")
    rdd.map(x => (x, x)).reduceByKey((x, y) => x + x, numExec).collect()
    rdd.repartition(numExec - 1).collect()
  }

  def main(args: Array[String]) {
    if (args.size < 2) {
      println(
        "mllib.perf.TestRunner requires 2 or more args, you gave %s, exiting".format(args.size))
      System.exit(1)
    }
    val proberLogFile = args(0)
    val testName = args(1)
    val perfTestArgs = args.drop(2)
    val sc = new SparkContext(new SparkConf().setAppName("TestRunner: " + testName))

    // Unfortunate copy of code because there are Perf Tests in both projects and the compiler doesn't like it
    val test: PerfTest = testName match {
      case "sparse-glm-regression" => new SparseGLMRegressionTest(sc)
      case "glm-regression" => new GLMRegressionTest(sc)
      case "glm-classification" => new GLMClassificationTest(sc)
      case "naive-bayes" => new NaiveBayesTest(sc)
      // recommendation
      case "als" => new ALSTest(sc)
      // clustering
      case "kmeans" => new KMeansTest(sc)
      // trees
      case "decision-tree" => new DecisionTreeTest(sc)
      // linalg
      case "svd" => new SVDTest(sc)
      case "pca" => new PCATest(sc)
      // stats
      case "summary-statistics" => new ColumnSummaryStatisticsTest(sc)
      case "pearson" => new PearsonCorrelationTest(sc)
      case "spearman" => new SpearmanCorrelationTest(sc)
      case "chi-sq-feature" => new ChiSquaredFeatureTest(sc)
      case "chi-sq-gof" => new ChiSquaredGoFTest(sc)
      case "chi-sq-mat" => new ChiSquaredMatTest(sc)
    }
    test.initialize(testName, perfTestArgs)
    // Generate a new dataset for each test
    val rand = new java.util.Random(test.getRandomSeed)

    val numTrials = test.getNumTrials
    val interTrialWait = test.getWait
    var proberLog: ProberResults = null

    val testOptions: JValue = test.getOptions
    val results: Seq[JValue] = (1 to numTrials).map { i =>
      test.createInputData(rand.nextLong())

      test match {
        case x: ClusteringTests => warmUpNetworkLinks(x.sc)
        case x: RegressionAndClassificationTests[_] => warmUpNetworkLinks(x.sc)
        case _ =>
      }

      // We register prober at last trial, so the log records
      // the DAG once.  Further, data generation & validation
      // are not included in the DAG (which consists of training only).
      if (i == numTrials) {
        Thread.sleep(3000)
        test.beforeProberJob(sc)
        test.recordAtTaskLevel()
        Thread.sleep(500)
      }

      val res = test.run()
      System.gc()
      Thread.sleep(interTrialWait)
      proberLog = res._2
      res._1
    }

    val parseType = (str: String) => str.trim.split(":")(1).trim
    val ec2DriverType = parseType("ec2-metadata -t".!!)
    val worker = "head -n1 /root/spark-ec2/slaves".!!.trim
    val ec2InstanceType = parseType(s"ssh $worker ec2-metadata -t".!!)

    // Report the test results as a JSON object describing the test options, Spark
    // configuration, Java system properties, as well as the per-test results.
    // This extra information helps to ensure reproducibility and makes automatic analysis easier.
    val json: JValue =
      ("testName" -> testName) ~
      ("options" -> testOptions) ~
      ("numExecutors" -> sc.getExecutorMemoryStatus.size) ~
      ("coresPerExecutor" -> System.getenv("SPARK_WORKER_CORES")) ~
      ("ec2InstanceType" -> ec2InstanceType) ~
      ("ec2DriverType" -> ec2DriverType) ~
      ("sparkConf" -> sc.getConf.getAll.toMap) ~
      ("sparkVersion" -> sc.version) ~
      ("systemProperties" -> System.getProperties.asScala.toMap) ~
      ("results" -> results)
    println("results: " + compact(render(json)))

    Option(proberLog).foreach(_.printToFile(proberLogFile))

    sc.stop()
  }
}
