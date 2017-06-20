package com.olol

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.{FileSystem, Path}
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
        else comp(array(i), array(i + 1)) && iter(i + 1)
      }

      iter(0)
    }

    val appConf = ConfigFactory.load()

    val path = args(0)

    val schemaPath = path.substring(0, path.lastIndexOf('/') + 1) + ".pig_header"

    val conf = new SparkConf().setAppName("Max Price").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val in = fs.open(new Path(schemaPath))
    val schema_txt = scala.io.Source.fromInputStream(in).mkString.trim()

    val sqlContext = new SQLContext(sc)

    val df_data = sqlContext.read.format("com.databricks.spark.csv")
      .option("delimiter", "\u0007")
      .option("header", "false")
      .option("inferSchema", "true")
      .load(path)

    val colName = schema_txt.split("\u0007")

    val df = df_data.toDF(colName: _*)

    val dateFieldName = "madmen_transaction_created_date"

    val active = df.schema.fields
      .filter(f => f.dataType == DoubleType || f.dataType == FloatType || f.dataType == IntegerType || f.dataType == LongType)
      .map(f => f.name)

    val mean = df.select(dateFieldName, active: _*).groupBy(dateFieldName).mean().orderBy(dateFieldName).drop(dateFieldName)

    val trans = mean.rdd.collect.map(row => row.toSeq.toList).toList.transpose

    val colNames = mean.columns
    val vals = trans.map(li => isSorted(li.toArray.map(_.asInstanceOf[Double])) || isSorted(li.toArray.map(_.asInstanceOf[Double]), true))

    println((colNames, vals).zipped.map((c, v) => (c.substring(4, c.length - 1), v)).mkString("\n"))

  }
}
