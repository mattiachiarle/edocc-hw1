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

/**
 * Function used to compute the similarity between two nodes. The lower the similarity the closer the nodes are.
 * My implementation considers both a comparison between the target nodes and a comparison of their neighbors to further improve the performances
 * @param node1 target node
 * @param nodes1 neighbors of the target node
 * @param node2 node with which we are performing the comparison
 * @param nodes2 neighbors of node 2
 * @return similarity score
 */
def ComputeSimilarity(node1: NodeObject, nodes1: Array[NodeObject], node2: NodeObject, nodes2: Array[NodeObject]): Double = {

  var result = ComputeValue(node1, node2)

  if (result == 0) { //If the nodes are exactly the same, we don't need to check the neighbors and we can directly return
    return result
  }

  nodes1.foreach(n1 => { //For each neighbor of the first node, we compute the lowest similarity with the neighbors of node 2
    var tmp = Double.MaxValue
    nodes2.foreach(n2 => {
      val res = ComputeValue(n1, n2)
      if (res < tmp) {
        tmp = res
      }
    })
    result = result + tmp //We add the lowest similarity to the current result
  })

  result / (nodes1.length) //We normalize the result

}

/**
 * Function to simply compute the similarity score between two nodes.
 * @param node1 first node
 * @param node2 second node
 * @return similarity score
 */
private def ComputeValue(node1: NodeObject, node2: NodeObject): Double = {

  //Properties considered: maxBranchingFactor, maxDepth, maxProperties, propValueRange, storedValue

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

/**
 * Function used to avoid divisions by 0
 * @param n1 first number
 * @param n2 second number
 * @return the maximum between n1 and n2 or 1 if they're both 0
 */
private def SafeDivision(n1: Double, n2: Double): Double = {
  if ((n1 != 0).||(n2 != 0)) {
    n1 max n2
  }
  else {
    1.0
  }
}

/*
    Mapper symbols:
      0=added
      1=removed
      2=modified
      3=same
 */

object GraphMapper:
  class Map extends MapReduceBase with Mapper[LongWritable, Text, LongWritable, Text]:
    @throws[IOException]
    override def map(key: LongWritable, value: Text, output: OutputCollector[LongWritable, Text], reporter: Reporter): Unit = {
      val logger = LoggerFactory.getLogger(getClass)
      val line = value.toString
      val elements = line.split("%%%")
      val analysisType = elements(0) //We understand which task we're executing

      val config = ConfigFactory.load()
      val threshold = config.getDouble("Comparison.threshold")

      if (analysisType == "1") {
        val optionOriginal = elements(1).split(" ").map(n => decode[NodeObject](n).toOption) //We retrieve all the original nodes
        val optionPerturbed = elements(2).split(" ").map(n => decode[NodeObject](n).toOption) //Same for the perturbed ones

        val original = optionOriginal.collect { case Some(nodeObject) => nodeObject } //To remove the option
        val perturbed = optionPerturbed.collect { case Some(nodeObject) => nodeObject }

        val similarity = ComputeSimilarity(original(0), original.drop(1), perturbed(0), perturbed.drop(1)) //We compute the similarity. We also remove the original and perturbed nodes analyzed from the array of neighbors 

        var result = 1 //In the task 1, the most common case is the one in which the node will result removed, or, in other words, different than the other node

        if (similarity == 0) {
          result = 3 //The nodes are the same
        }
        else if (similarity < threshold) {
          result = 2 //The nodes are different but similar enough
        }

        output.collect(new LongWritable(original(0).id), new Text(s"${result},${perturbed(0).id}"))
      }
      else {
        val optionPerturbed = elements(1).split(" ").map(n => decode[NodeObject](n).toOption)
        val optionOriginal = elements(2).split(" ").map(n => decode[NodeObject](n).toOption)

        val original = optionOriginal.collect { case Some(nodeObject) => nodeObject }
        val perturbed = optionPerturbed.collect { case Some(nodeObject) => nodeObject }

        val similarity = ComputeSimilarity(perturbed(0), perturbed.drop(1), original(0), original.drop(1))

        var result = 1 //Again removed is the most common case

        if (similarity >= threshold) {
          result = 0 //If two nodes are different we mark the new node as added
        }

        output.collect(new LongWritable(perturbed(0).id), new Text(s"${result},${original(0).id}"))
      }

    }

  class Reduce extends MapReduceBase with Reducer[LongWritable, Text, Text, Text]:
    override def reduce(key: LongWritable, values: util.Iterator[Text], output: OutputCollector[Text, Text], reporter: Reporter): Unit = {

      val logger = LoggerFactory.getLogger(getClass)

      val newValues = values.asScala.map(v => v.toString.split(","))

      var max = -1
      var node = 0

      newValues.foreach(v => { //We save the maximum value provided by the mapper and the node associated with it
        if(v(0).toInt > max){
          max = v(0).toInt
          node = v(1).toInt
        }
      })

      output.collect(new Text(s"${key},${max},${node}"), new Text("")) //Little trick used to achieve the CSV syntax. The best practice would be to have key=key, value = max,node
  }

  @main def runMapReduce(inputPath: String, outputPath: String) =

    val config = ConfigFactory.load()

    val nMappers = config.getString("MapReduce.nMappers")
    val nReducers = config.getString("MapReduce.nReducers")

    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName("GraphAnalysis")
    
    /*
      When running the program locally, uncomment the following line to enable auto deletion of output directory
    */
//    conf.set("fs.defaultFS", "file:///")

    conf.set("mapreduce.job.maps", nMappers)
    conf.set("mapreduce.job.reduces", nReducers)
    conf.setOutputKeyClass(classOf[LongWritable])
    conf.setOutputValueClass(classOf[Text])
    conf.setMapperClass(classOf[Map])
    conf.setReducerClass(classOf[Reduce])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, Text]])


    /*
      When running the program locally, uncomment the following line to enable auto deletion of output directory
    */
    
//    val fs = FileSystem.get(conf)
//
//    val output = new Path(outputPath)
//
//    if (fs.exists(output)) {
//      fs.delete(output, true)
//    }

    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    FileOutputFormat.setOutputPath(conf, new Path(outputPath))

    val logger = LoggerFactory.getLogger(getClass)

    logger.info(s"Starting mapreduce with ${nMappers} mappers and ${nReducers} reducers")

    JobClient.runJob(conf)

    logger.info("Mapreduce successfully ended!")
