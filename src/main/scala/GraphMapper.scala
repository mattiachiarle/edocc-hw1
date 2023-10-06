import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{DoubleWritable, IntWritable, LongWritable, Text}
import org.apache.hadoop.util.*
import org.apache.hadoop.conf.*
import org.apache.hadoop.io.*

import java.io.IOException
import java.util
//import org.apache.hadoop.mapreduce.Job
//import org.apache.hadoop.mapreduce.Mapper
//import org.apache.hadoop.mapreduce.Reducer
//import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
//import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import NetGraphAlgebraDefs.*
import org.apache.hadoop.mapred.*

import scala.collection.*
import scala.jdk.CollectionConverters.*
import GraphExplorer.*
import io.circe.*
import io.circe.generic.auto.*
import io.circe.parser.*
import io.circe.syntax.*
import org.apache.hadoop.mapred.{JobClient, JobConf}

object MapReduceProgram:
  class Map extends MapReduceBase with Mapper[LongWritable, Text, LongWritable, LongWritable]:
    @throws[IOException]
    override def map(key: LongWritable, value: Text, output: OutputCollector[LongWritable, LongWritable], reporter: Reporter): Unit = {
      println("Starting mapper")
      val line = value.toString
      val elements = line.split("%%%")
      val analysisType = elements(0)

      if (analysisType == "1") {
        val optionOriginal = elements(1).split(" ").map(n => decode[NodeObject](n).toOption)
        val optionPerturbed = elements(2).split(" ").map(n => decode[NodeObject](n).toOption)

        val original = optionOriginal.collect { case Some(nodeObject) => nodeObject }
        val perturbed = optionPerturbed.collect { case Some(nodeObject) => nodeObject }

        val similarity = ComputeSimilarity(original(0), original.drop(1), perturbed(0), perturbed.drop(1))

        var result = 0

        if (similarity == 0) {
          result = 3
        }
        else if (similarity < 0.1) {
          result = 2
        }

        //context.write(new LongWritable(original(0).id), new LongWritable(result));
        output.collect(new LongWritable(original(0).id), new LongWritable(result))
      }
      else {
        val optionPerturbed = elements(1).split(" ").map(n => decode[NodeObject](n).toOption)
        val optionOriginal = elements(2).split(" ").map(n => decode[NodeObject](n).toOption)

        val original = optionOriginal.collect { case Some(nodeObject) => nodeObject }
        val perturbed = optionPerturbed.collect { case Some(nodeObject) => nodeObject }

        val similarity = ComputeSimilarity(perturbed(0), perturbed.drop(1), original(0), original.drop(1))

        var result = 3

        if (similarity >= 0.1) {
          result = 1
        }

        //context.write(new LongWritable(perturbed(0).id), new LongWritable(result));
        output.collect(new LongWritable(perturbed(0).id), new LongWritable(result))
      }

    }

    private def ComputeSimilarity(node1: NodeObject, nodes1: Array[NodeObject], node2: NodeObject, nodes2: Array[NodeObject]): Double = {

      var result = ComputeValue(node1, node2)

      if (result == 0) {
        if (node1.id != node2.id) {
          println(s"Node1: ${node1.id} - ${node1.properties}. Node2: ${node2.id} - ${node2.properties}")
        }
        return result
      }

      nodes1.foreach(n1 => {
        var tmp = Double.MaxValue
        nodes2.foreach(n2 => {
          val res = ComputeValue(n1, n2)
          if (res < tmp) {
            tmp = res
          }
        })
        result = result + tmp
      })

      result / (nodes1.length min nodes2.length)

    }

    private def ComputeValue(node1: NodeObject, node2: NodeObject): Double = {

      //maxBranchingFactor, maxDepth, maxProperties, propValueRange, storedValue

      val cBranching = 0.03
      val cDepth = 0.4
      val cProperties = 0.4
      val cValueRange = 0.03
      val cStoredValue = 0.03

      cBranching * (node1.maxBranchingFactor - node2.maxBranchingFactor).abs / SafeDivision(node1.maxBranchingFactor, node2.maxBranchingFactor) +
        cDepth * (node1.maxDepth - node2.maxDepth).abs / SafeDivision(node1.maxDepth, node2.maxDepth) +
        cProperties * (node1.maxProperties - node2.maxProperties).abs / SafeDivision(node1.maxProperties, node2.maxProperties) +
        cValueRange * (node1.propValueRange - node2.propValueRange).abs / SafeDivision(node1.propValueRange, node2.propValueRange) +
        cStoredValue * (node1.storedValue - node2.storedValue).abs / SafeDivision(node1.storedValue, node2.storedValue)
    }

    private def SafeDivision(n1: Double, n2: Double): Double = {
      if ((n1 != 0).||(n2 != 0)) {
        n1 max n2
      }
      else {
        1.0
      }
  }

  class Reduce extends MapReduceBase with Reducer[LongWritable, LongWritable, LongWritable, LongWritable]:
    override def reduce(key: LongWritable, values: util.Iterator[LongWritable], output: OutputCollector[LongWritable, LongWritable], reporter: Reporter): Unit = {
  //    var max : Long = -1
  //    values.forEach(v => {
  //      val vint = v.get()
  //      if(vint > max){
  //        max = vint
  //      }
  //    })
      println("Starting reducer")

      val max = values.asScala.reduce((valueOne, valueTwo) => new LongWritable(valueOne.get() max valueTwo.get()))

      //context.write(key, new LongWritable(max))
      output.collect(key, max)
  }

  @main def runMapReduce(inputPath: String, outputPath: String) =
    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName("GraphAnalysis")
    conf.set("fs.defaultFS", "local")
    conf.set("mapreduce.job.maps", "1")
    conf.set("mapreduce.job.reduces", "1")
    conf.setOutputKeyClass(classOf[LongWritable])
    conf.setOutputValueClass(classOf[LongWritable])
    conf.setMapperClass(classOf[Map])
//    conf.setCombinerClass(classOf[Reduce])
    conf.setReducerClass(classOf[Reduce])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[LongWritable, LongWritable]])

    val fs = FileSystem.get(conf)

    val input = new Path(inputPath)
    val output = new Path(outputPath)

    if (fs.exists(output)) {
      fs.delete(output, true)
    }

    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    FileOutputFormat.setOutputPath(conf, new Path(outputPath))
    JobClient.runJob(conf)

  //  val input = new Path("./input")
  //  val output = new Path("./output")
  //
  //  val fs = FileSystem.get(conf)
  //
  //  if (fs.exists(output)) {
  //    fs.delete(output, true)
  //  }
  //
  //  FileInputFormat.addInputPath(job, input)
  //  FileOutputFormat.setOutputPath(job, output)
  //
  //  if (job.waitForCompletion(true)) {
  //    //logger.info("Job completed successfully")
  //    println("Success")
  //  } else {
  //    //logger.info("Job failed")
  //    println("Failure")
  //  }

