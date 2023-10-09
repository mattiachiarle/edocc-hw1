import java.util
import java.io.{FileOutputStream, ObjectOutputStream, PrintWriter}
import scala.collection.*
import org.apache.commons.io.FileUtils
import io.circe.*
import io.circe.generic.auto.*
import io.circe.parser.*
import io.circe.syntax.*
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapred.JobConf
import org.slf4j.LoggerFactory
import NetGraphAlgebraDefs.*
import com.typesafe.config.{Config, ConfigFactory}

object GraphSharding {
  @main def CreateShards(): Unit = {

    val config = ConfigFactory.load()
    val original = config.getString("Graphs.fileName") //One of the configuration parameters is the name of the file
    val perturbed = s"${original}.perturbed"
    val dir = config.getString("MapReduce.inputPath") //Where to save the shards
    val sourceDir = config.getString("MapReduce.graphLocation") //Where the graphs are located

    val originalGraph = NetGraph.load(original, sourceDir)
    val perturbedGraph = NetGraph.load(perturbed, sourceDir)

    val conf: JobConf = new JobConf(this.getClass)

    val fs = FileSystem.get(conf)

    val directory = new Path(dir)

    if (!fs.exists(directory)) {
      fs.mkdirs(directory) //If the input directory doesn't exist, we create it
    }

    val logger = LoggerFactory.getLogger(getClass)

    var out : PrintWriter = null

    (originalGraph, perturbedGraph) match {
      case (Some(originalGraph), Some(perturbedGraph)) =>

        logger.info(s"Starting the sharding operation. ${originalGraph.sm.nodes().size() + perturbedGraph.sm.nodes().size()} files will be created in ${dir}/")

        originalGraph.sm.nodes().forEach(on => {
          out = {
            new PrintWriter (s"${dir}/shards1-node${on.id}") //For each node we create a new file
          }

          val original = mutable.ArrayBuffer[NodeObject]()
          original += on //The node that is compared will always be the first element of the array
          originalGraph.sm.predecessors(on).forEach(n => original += n)
          originalGraph.sm.successors(on).forEach(n => original += n) //We retrieve all the successors and predecessors of the node, and we put them in an array
          perturbedGraph.sm.nodes().forEach(pn => { //We compare each node from the original graph with all the nodes of the perturbed graph
            val perturbed = mutable.ArrayBuffer[NodeObject]()
            perturbed += pn
            perturbedGraph.sm.predecessors(pn).forEach(n => perturbed += n)
            perturbedGraph.sm.successors(pn).forEach(n => perturbed += n)

            out.print("1%%%") //The initial number represents the tasks: 1 = comparison original-perturbed, 2 = comparison perturbed-original
            original.foreach(n => out.print(s"${n.asJson.noSpaces} ")) //We store the nodes as JSON objects with circe library, so that it'll be easy to retrieve them afterwards
            out.print("%%%") //%%% is the symbol used to split the different sections of each line
            perturbed.foreach(n => out.print(s"${n.asJson.noSpaces} "))
            out.print("\n")
          })
          out.close() //To flush the buffer and avoid errors due to the high number of open files
        })

        logger.info("Phase 1 completed - moving to phase 2")

        perturbedGraph.sm.nodes().forEach(pn => { //We perform the same instructions seen previously for task 2
          out = {
            new PrintWriter (s"${dir}/shards2-node${pn.id}")
          }

          val perturbed = mutable.ArrayBuffer[NodeObject]()
          perturbed += pn
          perturbedGraph.sm.predecessors(pn).forEach(n => perturbed += n)
          perturbedGraph.sm.successors(pn).forEach(n => perturbed += n)
          originalGraph.sm.nodes().forEach(on => {
            val original = mutable.ArrayBuffer[NodeObject]()
            original += on
            originalGraph.sm.predecessors(on).forEach(n => original += n)
            originalGraph.sm.successors(on).forEach(n => original += n)

            out.print("2%%%")
            perturbed.foreach(n => out.print(s"${n.asJson.noSpaces} "))
            out.print("%%%")
            original.foreach(n => out.print(s"${n.asJson.noSpaces} "))
            out.print("\n")
          })
          out.close()
        })
        logger.info("Sharding operation completed")
    }
  }
}
