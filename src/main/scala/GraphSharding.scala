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
import NetGraphAlgebraDefs._

object GraphSharding {
  def CreateShards(dir : String, original: String, perturbed: String): Unit = {
    val originalGraph = NetGraph.load(original, "./")
    val perturbedGraph = NetGraph.load(perturbed, "./")

    val conf: JobConf = new JobConf(this.getClass)

    val fs = FileSystem.get(conf)

    val directory = new Path(dir)

    if (!fs.exists(directory)) {
      fs.mkdirs(directory)
    }

    val logger = LoggerFactory.getLogger(getClass)

    var out : PrintWriter = null

    (originalGraph, perturbedGraph) match {
      case (Some(originalGraph), Some(perturbedGraph)) =>

        logger.info(s"Starting the sharding operation. ${originalGraph.sm.nodes().size() + perturbedGraph.sm.nodes().size()} files will be created in ${dir}/")

        originalGraph.sm.nodes().forEach(on => {
          out = {
            new PrintWriter (s"${dir}/shards1-node${on.id}")
          }

          val original = mutable.ArrayBuffer[NodeObject]()
          original += on
          originalGraph.sm.predecessors(on).forEach(n => original += n)
          originalGraph.sm.successors(on).forEach(n => original += n)
          perturbedGraph.sm.nodes().forEach(pn => {
            val perturbed = mutable.ArrayBuffer[NodeObject]()
            perturbed += pn
            perturbedGraph.sm.predecessors(pn).forEach(n => perturbed += n)
            perturbedGraph.sm.successors(pn).forEach(n => perturbed += n)

            out.print("1%%%")
            original.foreach(n => out.print(s"${n.asJson.noSpaces} "))
            out.print("%%%")
            perturbed.foreach(n => out.print(s"${n.asJson.noSpaces} "))
            out.print("\n")
          })
          out.close()
        })

        perturbedGraph.sm.nodes().forEach(pn => {
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
