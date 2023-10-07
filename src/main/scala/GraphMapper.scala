import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{DoubleWritable, IntWritable, LongWritable, Text}
import org.apache.hadoop.util.*
import org.apache.hadoop.conf.*
import org.apache.hadoop.io.*
import org.yaml.snakeyaml.Yaml
import scala.jdk.CollectionConverters._
//import scala.collection.JavaConverters._

import java.io.{File, FileInputStream, IOException, InputStreamReader}
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
import org.slf4j.LoggerFactory

object MapReduceProgram:
  class Map extends MapReduceBase with Mapper[LongWritable, Text, LongWritable, LongWritable]:
    @throws[IOException]
    override def map(key: LongWritable, value: Text, output: OutputCollector[LongWritable, LongWritable], reporter: Reporter): Unit = {
      val logger = LoggerFactory.getLogger(getClass)
      //logger.info(s"Starting mapper - string=${value.toString}")
      val line = value.toString
      val elements = line.split("%%%")
      val analysisType = elements(0)

      if (analysisType == "1") {
        val optionOriginal = elements(1).split(" ").map(n => decode[NodeObject](n).toOption)
        val optionPerturbed = elements(2).split(" ").map(n => decode[NodeObject](n).toOption)

        val original = optionOriginal.collect { case Some(nodeObject) => nodeObject }
        val perturbed = optionPerturbed.collect { case Some(nodeObject) => nodeObject }

        val similarity = ComputeSimilarity(original(0), original.drop(1), perturbed(0), perturbed.drop(1))

        var result = 1

        if (similarity == 0) {
          result = 3
        }
        else if (similarity < 0.1) {
          result = 2
        }

//        logger.info(s"Node ${original(0).id}: ${result}")

        //context.write(new LongWritable(original(0).id), new LongWritable(result));
        output.collect(new LongWritable(original(0).id), new LongWritable(result))
      }
      else {
        val optionPerturbed = elements(1).split(" ").map(n => decode[NodeObject](n).toOption)
        val optionOriginal = elements(2).split(" ").map(n => decode[NodeObject](n).toOption)

        val original = optionOriginal.collect { case Some(nodeObject) => nodeObject }
        val perturbed = optionPerturbed.collect { case Some(nodeObject) => nodeObject }

        val similarity = ComputeSimilarity(perturbed(0), perturbed.drop(1), original(0), original.drop(1))

        var result = 1

        if (similarity >= 0.1) {
//          logger.info(s"Similarity: ${similarity}")
          result = 0
        }

//        logger.info(s"Node ${perturbed(0).id}: ${result}")

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

    //Possible cases:
    //0=added
    //1=removed
    //2=modified
    //3=same

  class Reduce extends MapReduceBase with Reducer[LongWritable, LongWritable, LongWritable, LongWritable]:
    override def reduce(key: LongWritable, values: util.Iterator[LongWritable], output: OutputCollector[LongWritable, LongWritable], reporter: Reporter): Unit = {

      val logger = LoggerFactory.getLogger(getClass)

//      logger.info(s"Reducing for ${key} with:")
//      values.asScala.foreach(v => logger.info(s"${v}"))

      val newValues = values.asScala.map(v => v.get())

      val max = newValues.max

//      logger.info(s"Reducing for ${key} with:")
//      values.asScala.foreach(v => logger.info(s"${v}"))

//      logger.info(s"Reducing for ${key} completed, value: ${max}")
//      var max = -1;
//
//      values.asScala.foreach(v => {
//        if(v.get().toInt>max){
//          max=v.get().toInt
//        }
//      })

      output.collect(key, new LongWritable(max))
  }

  @main def runMapReduce(inputPath: String, outputPath: String) =
    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName("GraphAnalysis")
    conf.set("fs.defaultFS", "file:///")
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

    val logger = LoggerFactory.getLogger(getClass)

//    logger.info("Starting mapreduce")

    JobClient.runJob(conf)

//    logger.info("Mapreduce successfully ended!")

    val modified = mutable.ArrayBuffer[Int]()
    val added = mutable.ArrayBuffer[Int]()
    val removed = mutable.ArrayBuffer[Int]()

    new File("output").listFiles.filter(_.getName.startsWith("part")).foreach(f => {
      scala.io.Source.fromFile(f).getLines().foreach { line =>
        val res = line.split("\t")
        if(res(1).toInt == 1){
          removed += res(0).toInt
        }
        else if(res(1).toInt == 0){
          added += res(0).toInt
        }
        else if (res(1).toInt == 2) {
          modified += res(0).toInt
        }
      }
    })

    print("Removed: ")
    removed.foreach(r => print(s"${r}, "))
    print("\n")
    print("Added: ")
    added.foreach(a => print(s"${a}, "))
    print("\n")
    print("Modified: ")
    modified.foreach(m => print(s"${m}, "))
    print("\n")

    val cont = scala.io.Source.fromFile("NetGameSimNetGraph_17-09-23-17-16-02.ngs.yaml").mkString
    val correct = cont.replace("\t","    ")
    print(s"Cont: ${correct}")

    val yaml = new Yaml()

//    val data = yaml.load(correct).asInstanceOf[java.util.Map[String, Any]].asScala.toMap

    val parsedData = yaml.load(correct).asInstanceOf[java.util.Map[String, Object]]
    val nodesMap = parsedData.get("Nodes").asInstanceOf[java.util.Map[String, java.util.List[Int]]]
    val modifiedNodes = Option(nodesMap.get("Modified")).getOrElse(new java.util.ArrayList[Int]()).asScala.toList
    val removedNodes = Option(nodesMap.get("Removed")).getOrElse(new java.util.ArrayList[Int]()).asScala.toList

    modifiedNodes.foreach(m => {
      if(!modified.contains(m)){
        logger.error(s"Node ${m} is not among modified nodes")
      }
    })

    modified.foreach(m => {
      if (!modifiedNodes.contains(m)) {
        logger.error(s"Node ${m} is a wrong modified node")
      }
    })

    removedNodes.foreach(r => {
      if (!removed.contains(r)) {
        logger.error(s"Node ${r} is not among removed nodes")
      }
    })

    removed.foreach(r => {
      if (!removedNodes.contains(r)) {
        logger.error(s"Node ${r} is a wrong removed node")
      }
    })

    var CTL = 0
    var DTL = 0

    removed.foreach(r => {
      if(!removedNodes.contains(r)){
        CTL += 1 //If we marked a node as removed while it was left in the graph, we wrongly removed a traceability link
      }
      else{
        DTL += 1 //We correctly discarded a TL
      }
    })

    var WTL = 0
    //ATL = (total number - wrong_modified - wrong_removed)

    removedNodes.foreach(r => {
      if (!removed.contains(r)) {
        WTL += 1 //A node that was removed has been considered as modified/same, so we wrongly maintained a traceability link
      }
    })

    val BTL = CTL + WTL



//    addedNodes.foreach(a => {
//      if (!added.contains(a)) {
//        logger.error(s"Node ${a} is not among modified nodes")
//      }
//    })

