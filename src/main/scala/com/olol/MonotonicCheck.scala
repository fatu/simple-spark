package com.olol

import java.nio.file.{Files, Paths, StandardOpenOption}

import breeze.linalg.DenseMatrix
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author fatu
  */
object MonotonicCheck {

  def main(args: Array[String]) {

    def isSorted[A: Ordering](array: Array[A], desc: Boolean = false): Boolean = {
      val order = implicitly[Ordering[A]]
      val comp = if (desc) order.lt _ else order.gt _
      @annotation.tailrec
      def iter(i: Int): Boolean = {
        if (i >= array.length - 1) true
        else !comp(array(i), array(i + 1)) && iter(i + 1)
      }
      iter(0)
    }

    val appConf = ConfigFactory.load()

    val conf = new SparkConf().setAppName("Max Price").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    println(getClass.getResource("/test.data").getPath)

    val df = sqlContext.read.format("com.databricks.spark.csv")
      .option("delimiter", "\u0007")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(getClass.getResource("/test").getPath)

    val dateFieldName = "madmen_transaction_created_date";

    val active = df.schema.fields
      .filter(f => f.dataType == DoubleType || f.dataType == FloatType || f.dataType == IntegerType || f.dataType == LongType)
      .map(f => f.name)

    val mean = df.select(dateFieldName, active: _*).groupBy(dateFieldName).mean().orderBy(dateFieldName).drop(dateFieldName)

    val header = mean.columns

    val trans = mean.rdd.collect.map(row => row.toSeq.toList).toList.transpose
//    mean.schema.fields.foreach { f =>
//      val fName = f.name
//      val fType = f.dataType
//      val fVals = mean.select(fName).rdd.map(r=>r(0).asInstanceOf[Double]).collect()
//      val monotonic = isSorted(fVals) || isSorted(fVals, true)
//      monotonicCheck += (fName -> monotonic)
//    }
    val result = mean.columns.mkString("|") + "\n" + trans.map(li=>isSorted(li.toArray.map(_.asInstanceOf[Double]))).mkString("|")

    println(result)
//    Files.write(
//      Paths.get(appConf.getString("output.path")),
//      result.getBytes,
//      //StandardCharsets.UTF_8,
//      StandardOpenOption.CREATE)
  }
}
