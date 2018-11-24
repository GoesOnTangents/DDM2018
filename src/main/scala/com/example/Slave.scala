package com.example

import java.io.File

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.example.MasterActor.{LinearCombinationFound, PasswordFound, SlaveSubscription}
import com.example.SlaveActor.{CrackPasswordsInRange, SolveLinearCombinationInRange, Subscribe}
import com.typesafe.config.ConfigFactory

import scala.util.control.Breaks.{break, breakable}

object LCSWorker {
  final val props: Props = Props(new LCSWorker())
  final case class Start(genes: Array[String],s: Int, i: Int, j: Int)
  final case class Setup(MasterAddress: String)
}

class LCSWorker extends Actor {
  import LCSWorker._

  //default
  var masterActorAddress: String = "akka.tcp://MasterSystem@127.0.0.1:42000/user/MasterActor"

  override def receive: Receive = {
    case Start(genes,s,i,j) =>
      this.find_partner(genes,s,i,j)
    case Setup(masterAddress) =>
      this.setup(masterAddress)
  }

  def find_partner(genes: Array[String], s: Int, i: Int, j: Int): Unit ={
    for(gene <- genes){
      //
    }
  }

  def lcs(a: String, b: String): Unit ={
    lcsM(a.toList, b.toList).mkString.length()
  }

  //<<------LCS MAGIC------>>\\
  case class Memoized[A1, A2, B](f: (A1, A2) => B) extends ((A1, A2) => B) {
    val cache = scala.collection.mutable.Map.empty[(A1, A2), B]
    def apply(x: A1, y: A2) = cache.getOrElseUpdate((x, y), f(x, y))
  }

  lazy val lcsM: Memoized[List[Char], List[Char], List[Char]] = Memoized {
    case (_, Nil) => Nil
    case (Nil, _) => Nil
    case (x :: xs, y :: ys) if x == y => x :: lcsM(xs, ys)
    case (x :: xs, y :: ys)           => {
      (lcsM(x :: xs, ys), lcsM(xs, y :: ys)) match {
        case (xs, ys) if xs.length > ys.length => xs
        case (xs, ys)                          => ys
      }
    }
  }
  //<<------LCS MAGIC------>>\\

  def setup(masterAddress: String): Unit ={
    this.masterActorAddress = masterAddress
  }
}
object LinearCombinationWorker {
  final val props: Props = Props(new LinearCombinationWorker())
  final case class Start(passwords: Array[Int], i: Long, j: Long, target_sum: Long)
  final case class Setup(masterAddress: String)

  //all values inclusive
  //TODO: this is a (almost-)duplicate of PasswordWorker.range_split
  //TODO: if we got too much time, we could figure out how generics work in scala and reduce redundancy
  def range_split(min: Long, max: Long, num_batches: Int): Vector[Tuple2[Long,Long]] = {
    var result = Vector[Tuple2[Long,Long]]()

    val total_range = max - min + 1L

    val range_per_batch : Long = total_range / num_batches
    val additional_stuff_for_first_batch : Long = total_range % num_batches
    var i : Long = min
    var j : Long = min + range_per_batch - 1L + additional_stuff_for_first_batch
    for (_ <- 1 to num_batches) {
      result = result :+ (i, j)
      i = j + 1
      j += range_per_batch
    }

    result
  }
}

class LinearCombinationWorker extends Actor {
  import LinearCombinationWorker._

  //default
  var masterActorAddress: String = "akka.tcp://MasterSystem@127.0.0.1:42000/user/MasterActor"

  override def receive: Receive = {
    case Start(passwords, i, j, sum) =>
      this.solve_linear_combination(passwords, i, j, sum)
    case Setup(masterAddress) =>
      this.setup(masterAddress)
  }

  def setup(masterAddress: String): Unit ={
    this.masterActorAddress = masterAddress
  }

  def solve_linear_combination(passwords: Array[Int], min: Long, max: Long, target_sum: Long): Unit = {
    val masterActor = context.actorSelection(this.masterActorAddress)
    println(s"$this has started to go through passwords from $min to $max")

    var combination: Long = min
    while (combination <= max) {
      var sum = 0;
      breakable {
        for (index <- passwords.indices) {
          if ((combination & (1L << index)) > 0) {
            sum += passwords(index)
            if (sum > target_sum) {
              break
            }
          }
        }
      }
      if (sum == target_sum) {
        //Combination found
        println("found a combination: " + combination)
        masterActor ! LinearCombinationFound(combination)
      }
      combination += 1
    }

    context.stop(self)
  }
}

object PasswordWorker {
  final val props: Props = Props(new PasswordWorker())
  final case class Start(passwords: Array[String], i: Int, j: Int)
  final case class Setup(masterAddress: String)

  //all values inclusive
  def range_split(min: Int, max: Int, num_batches: Int): Vector[Tuple2[Int,Int]] = {
    var result = Vector[Tuple2[Int,Int]]()

    val total_range = max - min + 1

    val range_per_batch = total_range / num_batches
    val additional_stuff_for_first_batch = total_range % num_batches
    var i = min
    var j = min + range_per_batch - 1 + additional_stuff_for_first_batch
    for (_ <- 1 to num_batches) {
      result = result :+ (i, j)
      i = j + 1
      j += range_per_batch
    }

    result
  }

}
class PasswordWorker() extends Actor {
  import PasswordWorker._

  //default
  var masterActorAddress: String = "akka.tcp://MasterSystem@127.0.0.1:42000/user/MasterActor"
  def crack_passwords_in_range(passwords: Array[String], i: Int, j: Int) : Unit = {
    val masterActor = context.actorSelection(this.masterActorAddress)
    println(s"$this has started to go through passwords from $i to $j")
    //fancy cracking functionality
    //send each password

    var id  = 0
    for (password <- i until j){
      val hashed_password = hash(password.toString)
      //println(s"${hashed_password}")
      id = 0
      for (password_hash <- passwords) {
        if (password_hash == hashed_password) {
          masterActor ! PasswordFound(id, password)
          println("cracked password " + password_hash + ": " + password)
        }
        id += 1
      }
    }
    context.stop(self)
  }
  def setup(masterAddress: String): Unit ={
    this.masterActorAddress = masterAddress
  }

  def hash(s: String): String = {
    val m = java.security.MessageDigest.getInstance("SHA-256").digest(s.getBytes("UTF-8"))
    m.map("%02x".format(_)).mkString
  }

  override def receive: Receive = {
    case Start(passwords, i, j) =>
      this.crack_passwords_in_range(passwords,i,j)
    case Setup(masterAddress) =>
      this.setup(masterAddress)
  }
}

object SlaveActor {
  final case class CrackPasswordsInRange(passwords: Array[String], i: Int, j: Int)
  final case class SolveLinearCombinationInRange(passwords: Array[Int], min: Long, max: Long, target_sum: Long)
  final val props: Props = Props(new SlaveActor())
  final case class Subscribe(addr: String)
  final case class FindLCSInRange(genes: Array[String], i: Int , j: Int)

  final val num_local_workers = 2 //TODO: dont hardcode
}

class SlaveActor extends Actor {
  import SlaveActor._
  var masterActorAddress: String = ""

  override def receive: Receive = {
    case Subscribe(addr) =>
      this.subscribe(addr)
    case CrackPasswordsInRange(passwords, i, j) =>
      this.start_password_workers(passwords, i, j)
    case SolveLinearCombinationInRange(passwords, i, j, sum) =>
      this.start_linear_combination_workers(passwords, i, j, sum)
    case FindLCSInRange(genes, i, j) =>
      this.start_lcs_workers(genes,i,j)
  }

  def subscribe(addr: String) = {
    this.masterActorAddress = addr
    val selection = context.actorSelection(addr)
    selection ! SlaveSubscription
  }

  def start_password_workers(passwords: Array[String], min: Int, max: Int): Unit = {
    import PasswordWorker.{Start,Setup}


    val ranges = PasswordWorker.range_split(min, max, num_local_workers)
    for (i <- ranges.indices) {
      val worker = context.actorOf(PasswordWorker.props, "PasswordCrackerWorker" + i)
      worker ! Setup(this.masterActorAddress)
      worker ! Start(passwords, ranges(i)._1,ranges(i)._2)
    }
  }

  def start_linear_combination_workers(passwords: Array[Int], min: Long, max: Long, target_sum: Long): Unit = {
    import LinearCombinationWorker.Start

    val ranges = LinearCombinationWorker.range_split(min, max, num_local_workers)
    for (i <- ranges.indices) {
      val worker = context.actorOf(LinearCombinationWorker.props, "LinearCombinationWorker" + i)
      worker ! Start(passwords, ranges(i)._1,ranges(i)._2, target_sum)
    }
  }
  def start_lcs_workers(genes: Array[String],i: Int,j: Int): Unit = {
    //TODO:
  }
}

//legacy:
object Slave extends App {
  val config = ConfigFactory.parseFile(new File("application.conf")).getConfig("SlaveSystem")
  val system: ActorSystem = ActorSystem("SlaveSystem", config)
  val slaveActor: ActorRef = system.actorOf(SlaveActor.props, "SlaveActor")
  val masterActorAddress: String = "akka.tcp://MasterSystem@127.0.0.1:42000/user/MasterActor"
  slaveActor ! Subscribe(masterActorAddress)


}

