package GraphMapper

import NetGraphAlgebraDefs.*
import com.typesafe.config.ConfigFactory
import io.circe.*
import io.circe.generic.auto.*
import io.circe.parser.*
import io.circe.syntax.*
import org.apache.hadoop.conf.*
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.*
import org.apache.hadoop.mapred.*
import org.apache.hadoop.util.*
import org.slf4j.LoggerFactory
import org.yaml.snakeyaml.Yaml

import java.io.{File, FileInputStream, IOException, InputStreamReader}
import java.util
import scala.collection.*
import scala.jdk.CollectionConverters.*

def ComputeSimilarity(node1: NodeObject, nodes1: Array[NodeObject], node2: NodeObject, nodes2: Array[NodeObject]): Double = {

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

  val config = ConfigFactory.load()

  val cBranching = config.getDouble("Comparison.cBranching")
  val cDepth = config.getDouble("Comparison.cDepth")
  val cProperties = config.getDouble("Comparison.cProperties")
  val cValueRange = config.getDouble("Comparison.cValueRange")
  val cStoredValue = config.getDouble("Comparison.cStoredValue")

  cBranching * (node1.maxBranchingFactor - node2.maxBranchingFactor).abs / SafeDivision(node1.maxBranchingFactor, node2.maxBranchingFactor) +
    cDepth * (node1.maxDepth - node2.maxDepth).abs / SafeDivision(node1.maxDepth, node2.maxDepth) +
    cProperties * (node1.maxProperties - node2.maxProperties).abs / SafeDivision(node1.maxProperties, node2.maxProperties) +
    cValueRange * (node1.propValueRange - node2.propValueRange).abs / SafeDivision(node1.propValueRange, node2.propValueRange) +
    cStoredValue * (node1.storedValue - node2.storedValue).abs / SafeDivision(node1.storedValue, node2.storedValue)
}

def SafeDivision(n1: Double, n2: Double): Double = {
  if ((n1 != 0).||(n2 != 0)) {
    n1 max n2
  }
  else {
    1.0
  }
}


object GraphMapper:
  class Map extends MapReduceBase with Mapper[LongWritable, Text, LongWritable, Text]:
    @throws[IOException]
    override def map(key: LongWritable, value: Text, output: OutputCollector[LongWritable, Text], reporter: Reporter): Unit = {
      val logger = LoggerFactory.getLogger(getClass)
      val line = value.toString
      val elements = line.split("%%%")
      val analysisType = elements(0)

      val config = ConfigFactory.load()
      val threshold = config.getDouble("Comparison.threshold")

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
        else if (similarity < threshold) {
          result = 2
        }

        output.collect(new LongWritable(original(0).id), new Text(s"${result},${perturbed(0).id}"))
      }
      else {
        val optionPerturbed = elements(1).split(" ").map(n => decode[NodeObject](n).toOption)
        val optionOriginal = elements(2).split(" ").map(n => decode[NodeObject](n).toOption)

        val original = optionOriginal.collect { case Some(nodeObject) => nodeObject }
        val perturbed = optionPerturbed.collect { case Some(nodeObject) => nodeObject }

        val similarity = ComputeSimilarity(perturbed(0), perturbed.drop(1), original(0), original.drop(1))

        var result = 1

        if (similarity >= threshold) {
          result = 0
        }

        output.collect(new LongWritable(perturbed(0).id), new Text(s"${result},${original(0).id}"))
      }

    }

    //Possible cases:
    //0=added
    //1=removed
    //2=modified
    //3=same

  class Reduce extends MapReduceBase with Reducer[LongWritable, Text, LongWritable, Text]:
    override def reduce(key: LongWritable, values: util.Iterator[Text], output: OutputCollector[LongWritable, Text], reporter: Reporter): Unit = {

      val logger = LoggerFactory.getLogger(getClass)

      val newValues = values.asScala.map(v => v.toString.split(","))

      var max = -1
      var node = 0

      newValues.foreach(v => {
        if(v(0).toInt > max){
          max = v(0).toInt
          node = v(1).toInt
        }
      })

      output.collect(key, new Text(s"${max},${node}"))
  }

  @main def runMapReduce() =

    val config = ConfigFactory.load()

    val inputPath = config.getString("MapReduce.inputPath")
    val outputPath = config.getString("MapReduce.outputPath")
    val nMappers = config.getString("MapReduce.nMappers")
    val nReducers = config.getString("MapReduce.nReducers")

    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName("GraphAnalysis")
    conf.set("fs.defaultFS", "file:///")
    conf.set("mapreduce.job.maps", nMappers)
    conf.set("mapreduce.job.reduces", nReducers)
    conf.setOutputKeyClass(classOf[LongWritable])
    conf.setOutputValueClass(classOf[Text])
    conf.setMapperClass(classOf[Map])
    conf.setReducerClass(classOf[Reduce])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[LongWritable, Text]])

    val fs = FileSystem.get(conf)

    val output = new Path(outputPath)

    if (fs.exists(output)) {
      fs.delete(output, true)
    }

    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    FileOutputFormat.setOutputPath(conf, new Path(outputPath))

    val logger = LoggerFactory.getLogger(getClass)

    logger.info(s"Starting mapreduce with ${nMappers} mappers and ${nReducers} reducers")

    JobClient.runJob(conf)

    logger.info("Mapreduce successfully ended!")

    computeStatistics()


  def computeStatistics(): Unit = {

    val logger = LoggerFactory.getLogger(getClass)

    val config = ConfigFactory.load()
    val file = config.getString("Graphs.fileName")
    val outputDir = config.getString("MapReduce.outputPath")

    val cont = scala.io.Source.fromFile(s"${file}.yaml").mkString
    val correct = cont.replace("\t", "    ")
    logger.info(s"Golden set: ${correct}")

    val yaml = new Yaml()

    val parsedData = yaml.load(correct).asInstanceOf[java.util.Map[String, Object]]
    val nodesMap = parsedData.get("Nodes").asInstanceOf[java.util.Map[String, java.util.List[Any]]]
    val modifiedNodes = Option(nodesMap.get("Modified")).getOrElse(new java.util.ArrayList[Int]()).asScala.toList
    val removedNodes = Option(nodesMap.get("Removed")).getOrElse(new java.util.ArrayList[Int]()).asScala.toList
    val tmp_added = Option(nodesMap.get("Added")).getOrElse(new java.util.LinkedHashMap[Int, Int]()).asInstanceOf[java.util.LinkedHashMap[Int, Int]]
    val addedNodes = tmp_added.values().asScala.toList

    var CTL = 0 //removed by me but not in practice
    var WTL = 0 //removed in practice but not by me
    var DTL = 0 //discarded by me and in practice
    var ATL = 0 //correct TL

    new File(outputDir).listFiles.filter(_.getName.startsWith("part")).foreach(f => {
      scala.io.Source.fromFile(f).getLines().foreach { line =>
        val elem = line.split("\t")
        val key = elem(0).toInt
        val values = elem(1).split(",")
        val similarity = values(0).toInt
        val link = values(1).toInt

        if ((similarity == 2).||(similarity == 3)) {
          if (key == link) { //TL correct
            ATL += 1
          }
          else {
            WTL += 1 //I created a non existing TL
            logger.error(s"The program created a non existing TL between ${key} and ${link}")
          }
        }
        if (similarity == 0) {
          if (addedNodes.contains(key)) {
            ATL += 1 //We correctly recognized an added node
          }
          else {
            WTL += 1 //Since I marked a node as added even if it was already present in the original graph, I wrongly added a TL
            logger.error(s"${key} was wrongly marked as added")
          }
        }
        if (similarity == 1) {
          if (removedNodes.contains(key)) {
            DTL += 1 //We correctly recognized a removed node
          }
          else {
            CTL += 1 //We removed a TL that was existing
            logger.error(s"${key} was wrongly removed")
          }
        }
      }
    })

    val BTL = CTL + WTL
    val GTL = DTL + ATL
    val RTL = GTL + BTL

    val ACC = ATL.toDouble / RTL.toDouble
    val BTLR = WTL.toDouble / RTL.toDouble
    val VPR = (GTL - BTL).toDouble / (2 * RTL).toDouble + 0.5

    logger.info(s"CTL: ${CTL}, DTL: ${DTL}, WTL: ${WTL}, ATL: ${ATL}")

    logger.info(s"Statistics: ACC = ${ACC}, BTLR = ${BTLR}, VPR = ${VPR}")
  }


