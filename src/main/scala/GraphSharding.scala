import NetGraphAlgebraDefs.*

import java.io.{FileOutputStream, ObjectOutputStream, PrintWriter}
import scala.collection.*
import org.apache.commons.io.FileUtils
import io.circe._, io.circe.generic.auto._, io.circe.parser._, io.circe.syntax._

object GraphSharding {
  def CreateShards(dir : String, original: String, perturbed: String): Unit = {
    val originalGraph = NetGraph.load(original, "/Users/mattia/repositories/homework1/homework1/")
    val perturbedGraph = NetGraph.load(perturbed, "/Users/mattia/repositories/homework1/homework1/")

    (originalGraph, perturbedGraph) match {
      case (Some(originalGraph), Some(perturbedGraph)) =>
        originalGraph.sm.nodes().forEach(on => {
//          println(s"Original: ${on}")
          val original = mutable.ArrayBuffer[NodeObject]()
          original += on
          originalGraph.sm.predecessors(on).forEach(n => original += n)
          originalGraph.sm.successors(on).forEach(n => original += n)
//          println(original)
//          println(original(0))
          perturbedGraph.sm.nodes().forEach(pn => {
            val perturbed = mutable.ArrayBuffer[NodeObject]()
            perturbed += pn
            perturbedGraph.sm.predecessors(pn).forEach(n => perturbed += n)
            perturbedGraph.sm.successors(pn).forEach(n => perturbed += n)

            val out = {
              new PrintWriter(s"${dir}/${on.id}-${pn.id}-original")
            }
            out.print("1%%%")
            original.foreach(n => out.print(s"${n.asJson.noSpaces} "))
            out.print("%%%")
            perturbed.foreach(n => out.print(s"${n.asJson.noSpaces} "))
            out.close()
          })
        })

        perturbedGraph.sm.nodes().forEach(pn => {
          val perturbed = mutable.ArrayBuffer[NodeObject]()
          perturbed += pn
          perturbedGraph.sm.predecessors(pn).forEach(n => perturbed += n)
          perturbedGraph.sm.successors(pn).forEach(n => perturbed += n)
          originalGraph.sm.nodes().forEach(on => {
            val original = mutable.ArrayBuffer[NodeObject]()
            original += on
            originalGraph.sm.predecessors(on).forEach(n => original += n)
            originalGraph.sm.successors(on).forEach(n => original += n)

            val out = {
              new PrintWriter(s"${dir}/${on.id}-${pn.id}-perturbed")
            }
            out.print("2%%%")
            perturbed.foreach(n => out.print(s"${n.asJson.noSpaces} "))
            out.print("%%%")
            original.foreach(n => out.print(s"${n.asJson.noSpaces} "))
            out.close()
          })
        })

    }
  }
}
