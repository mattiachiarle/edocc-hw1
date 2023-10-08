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
import org.slf4j.LoggerFactory

import java.net.{InetAddress, NetworkInterface, Socket}
import scala.util.{Failure, Success}

import com.typesafe.config.Config

object Main {
  def main(args: Array[String]): Unit = {

    val config = ConfigFactory.load()
    val fileName = config.getString("Graphs.fileName")
    val directory = config.getString("MapReduce.inputPath")
    
    CreateShards(directory,fileName,s"${fileName}.perturbed")
  }
}