package GraphMapper

import GraphMapper.getClass
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory
import org.yaml.snakeyaml.Yaml

import java.io.File
import java.util
import scala.collection.*
import scala.jdk.CollectionConverters.*

object Statistics {
  @main def ComputeStatistics(): Unit = {
    val logger = LoggerFactory.getLogger(getClass)

    val config = ConfigFactory.load()
    val file = config.getString("Graphs.fileName")
    val outputDir = config.getString("MapReduce.outputPath")

    val cont = scala.io.Source.fromFile(s"./${file}.yaml").mkString //We get the yaml file
    val correct = cont.replace("\t", "    ") //To allow the parsing of the yaml, the library requires four spaces instead of tab.
    logger.info(s"Golden set: ${correct}")

    val yaml = new Yaml()

    val parsedData = yaml.load(correct).asInstanceOf[java.util.Map[String, Object]]
    val nodesMap = parsedData.get("Nodes").asInstanceOf[java.util.Map[String, java.util.List[Any]]]
    val modifiedNodes = Option(nodesMap.get("Modified")).getOrElse(new java.util.ArrayList[Int]()).asScala.toList
    val removedNodes = Option(nodesMap.get("Removed")).getOrElse(new java.util.ArrayList[Int]()).asScala.toList
    val tmp_added = Option(nodesMap.get("Added")).getOrElse(new java.util.LinkedHashMap[Int, Int]()).asInstanceOf[java.util.LinkedHashMap[Int, Int]]
    val addedNodes = tmp_added.values().asScala.toList //We need just the valued of the HashMap

    var CTL = 0 //removed by me but not in practice
    var WTL = 0 //removed in practice but not by me
    var DTL = 0 //discarded by me and in practice
    var ATL = 0 //correct TL

    new File(outputDir).listFiles.filter(_.getName.startsWith("part")).foreach(f => {
      scala.io.Source.fromFile(f).getLines().foreach { line =>
        val elem = line.trim.split(",") //Trim is needed to remove whitespaces that could cause exceptions while converting the values to integers
        val key = elem(0).toInt
        val similarity = elem(1).toInt
        val link = elem(2).toInt

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

}
