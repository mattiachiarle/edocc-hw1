import GraphExplorer.{GraphAnalyze, GraphTraverse, GraphView}
import GraphSharding.CreateShards
import NetGraphAlgebraDefs.GraphPerturbationAlgebra.ModificationRecord
import NetGraphAlgebraDefs.NetModelAlgebra.{actionType, outputDirectory}
import NetGraphAlgebraDefs.{GraphPerturbationAlgebra, NetGraph, NetModelAlgebra}
import NetModelAnalyzer.Analyzer
import Randomizer.SupplierOfRandomness
import Utilz.{ConfigReader, CreateLogger, NGSConstants}
import com.google.common.graph.ValueGraph

import java.util.concurrent.{ThreadLocalRandom, TimeUnit}
import scala.concurrent.{Await, ExecutionContext, Future}
import com.typesafe.config.ConfigFactory
import guru.nidi.graphviz.engine.Format
import org.slf4j.Logger

import java.net.{InetAddress, NetworkInterface, Socket}
import scala.util.{Failure, Success}

object Main {
  def main(args: Array[String]): Unit = {
//    GraphTraverse("NetGameSimNetGraph_19-09-23-19-38-07.ngs")
//      GraphView("NetGameSimNetGraph_17-09-23-17-16-02.ngs","NetGameSimNetGraph_17-09-23-17-16-02.ngs.perturbed")
//      GraphAnalyze("NetGameSimNetGraph_19-09-23-19-38-07.ngs","NetGameSimNetGraph_19-09-23-19-38-07.ngs.perturbed")
//      GraphAnalyze("NetGameSimNetGraph_17-09-23-17-16-02.ngs","NetGameSimNetGraph_17-09-23-17-16-02.ngs.perturbed")
      CreateShards("input","NetGameSimNetGraph_17-09-23-17-16-02.ngs","NetGameSimNetGraph_17-09-23-17-16-02.ngs.perturbed")
  }
}