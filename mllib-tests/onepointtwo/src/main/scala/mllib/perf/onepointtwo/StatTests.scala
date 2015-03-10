package mllib.perf.onepointtwo

import org.json4s.JsonDSL._
import org.json4s.JsonAST._

import scala.util.Random

import org.apache.spark.mllib.linalg.{Matrices, Vectors, Matrix, Vector}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD

import mllib.perf.onepointtwo.util.DataGenerator


/**
 * Parent class for the tests for the statistics toolbox
 */
abstract class StatTests[T](sc: SparkContext) extends PerfTest {

  def runTest(rdd: RDD[T])

  val NUM_ROWS =  ("num-rows",   "number of rows of the matrix")
  val NUM_COLS =  ("num-cols",   "number of columns of the matrix")

  longOptions = Seq(NUM_ROWS)
  intOptions = intOptions ++ Seq(NUM_COLS)

  var rdd: RDD[T] = _

  val options = intOptions ++ stringOptions  ++ booleanOptions ++ doubleOptions ++ longOptions
  addOptionsToParser()

  override def run() = {
    val start = System.currentTimeMillis()
    runTest(rdd)
    val end = System.currentTimeMillis()
    val time = (end - start).toDouble / 1000.0
    rdd.unpersist()
    (Map("time" -> time), proberResults().waitAndCopy(3000))
  }
}

abstract class CorrelationTests(sc: SparkContext) extends StatTests[Vector](sc){
  override def createInputData(seed: Long) = {
    val m: Long = longOptionValue(NUM_ROWS)
    val n: Int = intOptionValue(NUM_COLS)
    val numPartitions: Int = intOptionValue(NUM_PARTITIONS)

    rdd = RandomRDDs.normalVectorRDD(sc, m, n, numPartitions, seed).cache()

    // Materialize rdd
    println("Num Examples: " + rdd.count())
  }
}

class PearsonCorrelationTest(sc: SparkContext) extends CorrelationTests(sc) {
  override def runTest(data: RDD[Vector]) {
     Statistics.corr(data)
  }
}

class SpearmanCorrelationTest(sc: SparkContext) extends CorrelationTests(sc) {
  override def runTest(data: RDD[Vector]) {
    Statistics.corr(data, "spearman")
  }
}

class ChiSquaredFeatureTest(sc: SparkContext) extends StatTests[LabeledPoint](sc) {
  override def createInputData(seed: Long) = {
    val m: Long = longOptionValue(NUM_ROWS)
    val n: Int = intOptionValue(NUM_COLS)
    val numPartitions: Int = intOptionValue(NUM_PARTITIONS)

    rdd = DataGenerator.generateClassificationLabeledPoints(sc, m, n, 0.5, 1.0, numPartitions,
      seed, chiSq = true).cache()

    // Materialize rdd
    println("Num Examples: " + rdd.count())
  }
  override def runTest(data: RDD[LabeledPoint]) {
    Statistics.chiSqTest(data)
  }
}

class ChiSquaredGoFTest(sc: SparkContext) extends StatTests[Vector](sc) {
  override def createInputData(seed: Long) = {
    val m: Long = longOptionValue(NUM_ROWS)
    val rng = new Random(seed)

    rdd = sc.parallelize(Seq(Vectors.dense(Array.fill(m.toInt)(rng.nextDouble()))), 1)
  }
  override def runTest(data: RDD[Vector]) {
    Statistics.chiSqTest(data.collect()(0))
  }
}

class ChiSquaredMatTest(sc: SparkContext) extends StatTests[Matrix](sc) {
  override def createInputData(seed: Long) = {
    val m: Long = longOptionValue(NUM_ROWS)
    val rng = new Random(seed)

    rdd = sc.parallelize(Seq(Matrices.dense(m.toInt, m.toInt, Array.fill(m.toInt * m.toInt)(rng.nextDouble()))), 1)
  }
  override def runTest(data: RDD[Matrix]) {
    Statistics.chiSqTest(data.collect()(0))
  }
}
