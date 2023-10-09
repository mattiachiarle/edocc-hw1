package GraphMapper

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import NetGraphAlgebraDefs.*
import GraphMapper.*
import com.typesafe.config.ConfigFactory
import Statistics.*

import scala.Double.NaN
import scala.collection.mutable

/**
 * In the tests I focused only on the function that computes the similarity. In fact, this is the critical part of the algorithm, and it's important to have those test to perform regression testing.
 */
class GraphMapperTest extends AnyFlatSpec with Matchers{
  val logger: Logger = LoggerFactory.getLogger(getClass)
  behavior of "Compute similarity"

  it should "provide similarity=0 with two identical nodes" in {
    val node = new NodeObject(1,1,1,1,1,1,1,1,2.0)

    val res = ComputeSimilarity(node,Array.empty,node,Array.empty)

    res shouldEqual(0)
  }

  it should "provide similarity<threshold with a minor modification of a node" in {
    val node1 = new NodeObject(1, 1, 1, 1, 1, 1, 1, 1, 2.0)
    val node2 = new NodeObject(2, 1, 1, 1, 1, 1, 1, 1, 1.99999999999)
    val node3 = new NodeObject(3, 4, 6, 2, 4, 1, 3, 10, 6.0)


    val config = ConfigFactory.load()
    val threshold = config.getDouble("Comparison.threshold")

    val array1 = new Array[NodeObject](1)
    val array2 = new Array[NodeObject](1)

    array1(0) = node3
    array2(0) = node3

    val res = ComputeSimilarity(node1, array1, node2, array2)

    res should be < threshold
  }

  it should "provide similarity>threshold with completely different nodes" in {
    val node1 = new NodeObject(1, 1, 1, 1, 1, 1, 1, 1, 2.0)
    val node2 = new NodeObject(2, 10, 15, 23, 34, 16, 100, 12, 19999.9999999)
    val node3 = new NodeObject(3, 4, 6, 2, 4, 1, 3, 10, 6.0)


    val config = ConfigFactory.load()
    val threshold = config.getDouble("Comparison.threshold")

    val array1 = new Array[NodeObject](1)
    val array2 = new Array[NodeObject](1)

    array1(0) = node3
    array2(0) = node3

    val res = ComputeSimilarity(node1, array1, node2, array2)

    res should be >= threshold
  }

  it should "provide similarity>threshold with similar nodes but highly different neighbors" in {
    val node1 = new NodeObject(1, 1, 1, 1, 1, 1, 1, 1, 2.0)
    val node2 = new NodeObject(2, 1, 1, 1, 1, 1, 1, 1, 1.99999999999)
    val node3 = new NodeObject(3, 4, 6, 2, 4, 1, 3, 10, 6.0)
    val node4 = new NodeObject(2, 10, 15, 23, 34, 16, 100, 12, 19999.9999999)


    val config = ConfigFactory.load()
    val threshold = config.getDouble("Comparison.threshold")

    val array1 = new Array[NodeObject](1)
    val array2 = new Array[NodeObject](1)

    array1(0) = node3
    array2(0) = node4

    val res = ComputeSimilarity(node1, array1, node2, array2)

    res should be >= threshold
  }

  it should "not crash while making a division by 0" in {
    val node1 = new NodeObject(1, 1, 1, 1, 1, 1, 1, 1, 0)
    val node2 = new NodeObject(2, 1, 1, 1, 1, 1, 1, 1, 0)
    val node3 = new NodeObject(3, 4, 6, 2, 4, 1, 3, 10, 0)
    val node4 = new NodeObject(2, 10, 15, 23, 34, 16, 100, 12, 0)


    val config = ConfigFactory.load()
    val threshold = config.getDouble("Comparison.threshold")

    val array1 = new Array[NodeObject](1)
    val array2 = new Array[NodeObject](1)

    array1(0) = node3
    array2(0) = node4

    val res = ComputeSimilarity(node1, array1, node2, array2)

    res should not be NaN
  }
}
