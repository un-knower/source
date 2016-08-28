package com.boc.iff

import java.lang.management.ManagementFactory

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by cvinc on 2016/6/7.
  */

trait SparkJobConfig extends AppConfig{

  var local: Boolean = false
  var repartition: Option[Int] = None

  override protected def makeOptions(optionParser: scopt.OptionParser[_]) = {
    super.makeOptions(optionParser)
    optionParser.opt[Unit]("local")
      .text("Local")
      .foreach{ x=>this.local=true }
    optionParser.opt[Int]("repartition")
      .text("Repartition")
      .foreach{ x=>this.repartition=Option(x) }
  }

  override def toString = {
    val builder = new mutable.StringBuilder(super.toString)
    if(builder.nonEmpty) builder ++= "\n"
    builder ++= "Local: %s\n".format(local.toString)
    builder ++= "Repartition: %s".format(repartition.toString)
    builder.toString
  }

}

trait SparkJob[T<:SparkJobConfig] {

  protected var sparkConf: SparkConf = null
  protected var sparkContext: SparkContext = null

  protected var dynamicAllocation: Boolean = false
  protected var numExecutors: Int = 0
  protected var maxExecutors: Int = 0
  protected var minExecutors: Int = 0

  protected def kryoClasses: Array[Class[_]] = Array[Class[_]]()

  protected def runOnSpark(jobConfig: T): Unit

  def start(jobConfig: T, args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    if (jobConfig.parse(args)) {
      sparkConf = new SparkConf().setAppName(jobConfig.appName)
      if (jobConfig.local) {
        sparkConf.setMaster("local").set("spark.executor.memory", "8g")
      }
      dynamicAllocation = sparkConf.getBoolean("spark.dynamicAllocation.enabled", dynamicAllocation)
      println("Dynamic Allocation: %s".format(String.valueOf(dynamicAllocation)))
      if(dynamicAllocation) {
        maxExecutors = sparkConf.getInt("spark.dynamicAllocation.maxExecutors", maxExecutors)
        minExecutors = sparkConf.getInt("spark.dynamicAllocation.minExecutors", minExecutors)
        println("Max Executors: " + maxExecutors)
        println("Min Executors: " + minExecutors)
      }else{
        numExecutors = sparkConf.getInt("spark.executor.instances", numExecutors)
        println("Num Executors: " + numExecutors)
      }
      sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      sparkConf.registerKryoClasses(kryoClasses)
      sparkContext = new SparkContext(sparkConf)
      try {
        println("--- Start " + this.getClass.getSimpleName + " Spark Job ---")
        System.setProperty("applicationId", sparkContext.applicationId)
        println("Application ID: " + sparkContext.applicationId)
        val processName = ManagementFactory.getRuntimeMXBean.getName
        println("Process: " + processName)
        val atPos = processName.indexOf("@")
        val pid =
          if(atPos == -1) processName
          else processName.substring(0, atPos)
        System.setProperty("pid", pid)
        println(jobConfig)
        val timeBegin = System.currentTimeMillis()
        runOnSpark(jobConfig)
        println("%s Completed! Time taken %d ms"
          .format(sparkContext.applicationId, System.currentTimeMillis() - timeBegin))
      } catch {
        case t: Throwable => throw t
      }finally {
        println("Shutting down job")
        Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
        sparkContext.stop()
      }
    }
  }
}