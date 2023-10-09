import NetGraphAlgebraDefs._
import scala.collection._

/**
 * Testing functions used to understand the logic of NetGameSim. Please don't consider them for the evaluation.
 */
object GraphExplorer {
  def GraphTraverse(filename: String): Unit = {
    val graph = NetGraph.load(filename, "/Users/mattia/repositories/homework1/homework1/")
    graph match {
      case Some(graph) =>
        val nodes = graph.sm.nodes()
        val edges = graph.sm.edges()

        val queue = mutable.Queue[Int]()
        val visited = mutable.Set[Int]()

        queue.enqueue(0)
        visited.add(0)
        while (queue.nonEmpty) {
          val node = queue.dequeue()
          println(s"Visiting ${node}")
          edges.forEach(e => {
            if (e.source.id.==(node).&&(!visited.contains(e.target.id))) {
              queue.enqueue(e.target.id)
              visited.add(e.target.id)
            }
          })
        }
    }
  }

  def GraphView(original: String, perturbed: String): Unit = {
    var graph = NetGraph.load(original, "/Users/mattia/repositories/homework1/homework1/")
    graph match {
      case Some(graph) =>
        val nodes = graph.sm.nodes()
        println("Original")
        nodes.forEach(n => println(s"Node ${n.id}: ${n.properties}"))
    }

    graph = NetGraph.load(perturbed, "/Users/mattia/repositories/homework1/homework1/")
    graph match {
      case Some(graph) =>
        val nodes = graph.sm.nodes()
        println("Perturbed")
        nodes.forEach(n => println(s"Node ${n.id}: ${n.properties}"))
    }
  }
}