package com.example

import java.io.File

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.example.MasterActor.{LinearCombinationFound, PasswordFound, SlaveSubscription}
import com.example.SlaveActor.{CrackPasswordsInRange, NoLinearCombinationFound, SolveLinearCombinationInRange, Subscribe}
import com.typesafe.config.ConfigFactory

import scala.util.control.Breaks.{break, breakable}

object LinearCombinationWorker {
  final val props: Props = Props(new LinearCombinationWorker())
  final case class Start(range: Tuple2[Long,Long])
  final case class Setup(masterAddress: String, passwords: Array[Int], target_sum: Long)

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
  var passwords: Array[Int] = Array[Int]()
  var target_sum: Long = -1L

  override def receive: Receive = {
    case Start(range) =>
      this.solve_linear_combination(range)
    case Setup(masterAddress, passwords, target_sum) =>
      this.setup(masterAddress, passwords, target_sum)
  }

  def setup(masterAddress: String, passwords: Array[Int], target_sum: Long): Unit ={
    this.masterActorAddress = masterAddress
    this.passwords = passwords
    this.target_sum = target_sum
  }

  def solve_linear_combination(range: Tuple2[Long, Long]): Unit = {
    val masterActor = context.actorSelection(this.masterActorAddress)
    val min = range._1
    val max = range._2
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
        return
      }
      combination += 1
    }

    context.parent ! NoLinearCombinationFound //TODO: is this good style, or should we do ActorSelection("SlaveActor")?
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
  final val props: Props = Props(new SlaveActor())

  final case class Subscribe(addr: String)

  final case class CrackPasswordsInRange(passwords: Array[String], i: Int, j: Int)

  final case class SolveLinearCombinationInRange(passwords: Array[Int], min: Long, max: Long, target_sum: Long)
  final case object StopSolvingLinearCombination
  final case object NoLinearCombinationFound

  final val num_local_workers = 2 //TODO: dont hardcode
}

class SlaveActor extends Actor {
  import SlaveActor._
  var masterActorAddress: String = ""

  var linear_combination_ranges : Vector[Tuple2[Long,Long]] = Vector[Tuple2[Long,Long]]()
  var linear_combination_next_index = 0
  var linear_combination_actors : Vector[ActorRef] = Vector[ActorRef]()


  override def receive: Receive = {
    case Subscribe(addr) =>
      this.subscribe(addr)
    case CrackPasswordsInRange(passwords, i, j) =>
      this.start_password_workers(passwords, i, j)
    case SolveLinearCombinationInRange(passwords, i, j, sum) =>
      this.start_linear_combination_workers(passwords, i, j, sum)
    case NoLinearCombinationFound =>
      this.distribute_linear_combination_work_package
    case StopSolvingLinearCombination =>
      this.stop_solving_linear_combination
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
      worker ! Setup(masterActorAddress)
      worker ! Start(passwords, ranges(i)._1,ranges(i)._2)
    }
  }

  def start_linear_combination_workers(passwords: Array[Int], min: Long, max: Long, target_sum: Long): Unit = {
    import LinearCombinationWorker.{Setup, Start}

    //we want to get ranges of a fixed size, so that workers dont get stuck working on too huge datasets
    //and can abort faster if they receive the corresponding message
    //TODO: in theory, the line below could fail if the second parameter is bigger than Int.maxValue
    //TODO: however, this won't happen in our scenario, so we ignore it for now
    val num_ranges: Int = math.max(num_local_workers, ((max - min + 1L) / 10000000L).toInt)
    linear_combination_ranges = LinearCombinationWorker.range_split(min, max, num_ranges)
    for (i <- 0 until num_local_workers) {
      val actor = context.actorOf(LinearCombinationWorker.props, "LinearCombinationWorker" + i)
      actor ! Setup(masterActorAddress, passwords, target_sum)
      actor ! Start(linear_combination_ranges(i))
      linear_combination_actors = linear_combination_actors :+ actor
    }
    linear_combination_next_index = num_local_workers
  }

  def distribute_linear_combination_work_package(): Unit = {
    import LinearCombinationWorker.Start
    if (linear_combination_next_index >= linear_combination_ranges.length) {
      println(s"${this.sender()} finished, but all work packages are already given out")
      return
    }

    this.sender() ! Start(linear_combination_ranges(linear_combination_next_index))
    linear_combination_next_index += 1
  }

  def stop_solving_linear_combination(): Unit = {
    println("stop_solving_linear_combination called")
    for (actor <- linear_combination_actors) {
      println(s"stopping $actor")
      context.stop(actor) //TODO good style, or rather PoisonPill?

    }
  }
}
