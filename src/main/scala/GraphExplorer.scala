import NetGraphAlgebraDefs._
import scala.collection._

object GraphExplorer {
  def GraphTraverse(filename: String): Unit = {
    val graph = NetGraph.load(filename,"/Users/mattia/repositories/homework1/homework1/")
    graph match {
      case Some(graph) =>
        val nodes = graph.sm.nodes()
        val edges = graph.sm.edges()

        val queue = mutable.Queue[Int]()
        val visited = mutable.Set[Int]()

        queue.enqueue(0)
        visited.add(0)
        while(queue.nonEmpty){
          val node = queue.dequeue()
          println(s"Visiting ${node}")
          edges.forEach(e => {
            if(e.source.id.==(node).&&(!visited.contains(e.target.id))){
              queue.enqueue(e.target.id)
              visited.add(e.target.id)
            }
          })
        }

        //queue.put(0)
        //while(!queue.empty){
          //print("visited queue.top")
          // node = queue.pop()
          //edges.filter(e => e.source==node).forEach(e => if(e.target.id not in queue){queue.put(e.target)})

        // }

//        while()
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

  def GraphAnalyze(original:String, perturbed:String): Unit = {
    val originalGraph = NetGraph.load(original, "/Users/mattia/repositories/homework1/homework1/")
    val perturbedGraph = NetGraph.load(perturbed, "/Users/mattia/repositories/homework1/homework1/")

    //If we analyze the original graph, if a node is different from all the nodes in perturbed it is a removed node
    //If we analyze the perturbed graph, if a node is different from all the nodes in original it is an added node

    //Possible cases:
    //0=removed
    //1=added
    //2=modified
    //3=same

    (originalGraph, perturbedGraph) match {
      case (Some(originalGraph), Some(perturbedGraph)) =>
        val OriginalNodes = originalGraph.sm.nodes()
        val PerturbedNodes = perturbedGraph.sm.nodes()

        val mod = mutable.Set[Int]()
        val rem = mutable.Set[Int]()

        OriginalNodes.forEach(on => {
          var result = 0
          PerturbedNodes.forEach(pn => {
            //println(s"Similarity between ${on.id} and ${pn.id}: ${ComputeSimilarity(on,pn)}")
            val onodes = mutable.Set[NodeObject]()
            val pnodes = mutable.Set[NodeObject]()
            originalGraph.sm.predecessors(on).forEach(n => onodes.add(n))
            originalGraph.sm.successors(on).forEach(n => pnodes.add(n))
            perturbedGraph.sm.predecessors(pn).forEach(n => pnodes.add(n))
            perturbedGraph.sm.successors(pn).forEach(n => pnodes.add(n))
            val similarity = ComputeSimilarity(on,onodes,pn,pnodes)
            if(similarity == 0){
              result=3
            }
            else if((similarity < 0.1).&&(result != 3)){
              result=2
            }
          })
//          result match{
//            case 0 =>
//              println(s"Node ${on.id} removed")
//            case 2 =>
//              println(s"Node ${on.id} modified")
//          }
          if(result == 0){
            rem.add(on.id)
          }
          if (result == 2) {
            mod.add(on.id)
          }
        })

        println("Modified: ")
        mod.foreach(m => print(s"${m}, "))
        println("")
        println("Removed: ")
        rem.foreach(r => print(s"${r}, "))
    }

  }

  def ComputeSimilarity(node1 : NodeObject, nodes1 : Set[NodeObject], node2 : NodeObject, nodes2 : Set[NodeObject]): Double = {

    var result = ComputeValue(node1, node2)

    if(result==0){
      if(node1.id != node2.id) {
        println(s"Node1: ${node1.id} - ${node1.properties}. Node2: ${node2.id} - ${node2.properties}")
      }
      return result
    }

    nodes1.foreach(n1 => {
      var tmp = Double.MaxValue
      nodes2.foreach(n2 => {
        val res = ComputeValue(n1,n2)
        if(res < tmp){
          tmp = res
        }
      })
      result = result + tmp
    })

    result/(nodes1.size min nodes2.size)

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
    if((n1 != 0).||(n2 != 0)){
      n1 max n2
    }
    else{
      1.0
    }
  }
}
