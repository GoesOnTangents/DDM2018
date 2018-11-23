package com.example

import java.io.File

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.example.MasterActor.Read
import com.example.SlaveActor.{CrackPasswordsInRange, SolveLinearCombinationInRange}
import com.typesafe.config.ConfigFactory

import scala.util.control.Breaks.{break, breakable}

object MasterActor {
  final val props: Props = Props(new MasterActor())
  case class Read(filename: String)
  case object CrackPasswords
  case object SlaveSubscription
  case object PasswordFound
  case object SolveLinearCombination
  case class LinearCombinationFound(combination: Long)
  case class PasswordFound(index: Int, password: Int)
}

class MasterActor extends Actor {

  import MasterActor._
  var expectedSlaveAmount: Int = 2
  var slaves: Array[ActorRef] = Array()

  var names: Array[String] = Array()
  var hashes: Array[String] = Array()
  var gene: Array[String] = Array()

  //results
  var cracked_passwords: Array[Int] = Array()
  var lcs_index: Array[Int] = Array()
  var linear_combination: Array[Boolean] = Array()
  var partner_hashes: Array[String] = Array()

  //counter variables
  var num_cracked_passwords = 0
  var linear_combination_found = false

  def read(filename: String): Unit = {
    val file_contents =
      scala.io.Source.fromFile(filename).getLines().drop(1)
    breakable {
      for (line <- file_contents) {
        if (line == "") break
        val cols = line.split(";").map(_.trim)
        this.names = this.names :+ cols(1)
        this.hashes = this.hashes :+ cols(2)
        this.gene = this.gene :+ cols(3)

        //println(s"${cols(0)}|${cols(1)}|${cols(2)}|${cols(3)}")
      }
    }
    val num_lines = names.length
    cracked_passwords = Array.ofDim(num_lines)
    linear_combination = Array.ofDim(num_lines)
    lcs_index = Array.ofDim(num_lines)
    partner_hashes = Array.ofDim(num_lines)
  }

  override def receive: Receive = {
    case SlaveSubscription =>
      this.subscribe_slaves()
    case CrackPasswords =>
      this.delegate_password_cracking()
    case PasswordFound(id, pw) =>
      this.store_password(id,pw)
    case SolveLinearCombination =>
      this.delegate_linear_combination()
    case LinearCombinationFound(combination: Long) =>
      this.store_linear_combination(combination)
    case Read(filename) =>
      this.read(filename)
    case msg: Any => throw new RuntimeException("unknown message type " + msg);

  }

  def delegate_password_cracking(): Unit = {
    println("Delegating Passwords to crack.")
    val ranges = PasswordWorker.range_split(100000, 999999, slaves.length)
    for (i <- slaves.indices) {
      slaves(i) ! CrackPasswordsInRange(hashes, ranges(i)._1, ranges(i)._2)
    }
  }

  def subscribe_slaves(): Unit = {
    slaves = slaves :+ sender()
    println(s"Current master's slaves:\n ${slaves.deep.mkString("\n")}")
    if (slaves.length == expectedSlaveAmount) {
      self ! CrackPasswords
    }
  }

  def store_password(id: Int, password: Int): Unit = {
    print(".")
    cracked_passwords(id) = password
    num_cracked_passwords += 1

    if (num_cracked_passwords == cracked_passwords.length) {
      self ! SolveLinearCombination
      println(s"\nAll passwords cracked:\n ${cracked_passwords.deep.mkString(",")},\n")
    }
  }

  def delegate_linear_combination(): Unit = {
    println("Delegating linear combinations to test.")

    //we assume that the sum is always an even number and we thus can divide by 2 - otherwise a partition cannot exist anyway
    val target_sum: Long = cracked_passwords.sum / 2
    println(s"total sum is ${2*target_sum}, so we look for $target_sum as sum")
    val ranges = LinearCombinationWorker.range_split(0L, (1L << cracked_passwords.length) - 1, slaves.length)
    for (i <- slaves.indices) {
      slaves(i) ! SolveLinearCombinationInRange(cracked_passwords, ranges(i)._1, ranges(i)._2, target_sum)
    }
  }

  def store_linear_combination(combination: Long): Unit = {
    if (linear_combination_found) {
      var sum: Long = 0
      var alternateSum: Long = 0
      for (index <- linear_combination.indices) {
        if ((combination & (1L << index)) > 0) {
          sum += cracked_passwords(index)
        } else {
          alternateSum += cracked_passwords(index)
        }
      }
      println(s"Already found a linear combination, so ignoring the new combination $combination, however sum is $sum and alternateSum is $alternateSum")
      return
    }

    var sum: Long = 0
    for (index <- linear_combination.indices) {
      linear_combination(index) = (combination & (1L << index)) > 0
      if (linear_combination(index)) {
        sum += cracked_passwords(index)
      }
    }
    println(s"sum is $sum")
    linear_combination_found = true

    //TODO trigger next step
    println("TODO: trigger gene subsequence tasks")
  }


  def findLcsPartners(): Unit = {
    //TODO: Step 1: Give slaves genes
    //Step 2: Make threadsafe Index and start distributing one name per slaves.
  }

  def storePartnerAndReassignName(): Unit = {
    //TODO: 1. Store Partner
    //2. IF work exists: Distribute new name
    //   IF not_yet_finished: Do nothing
    //   ELSE start find_linear_combination()
  }



  def find_prefixed_hashes(): Unit = {
    // Give slaves names
    // Distribute new names when finished...
  }
}

//legacy
object Master extends App {
    if (args.length == 0) {
      println("dude, you didn't give me any parameters")
    }
  val config = ConfigFactory.parseFile(new File("application.conf")).getConfig("MasterSystem")

  val system: ActorSystem = ActorSystem("MasterSystem", config)

  val masterActor: ActorRef = system.actorOf(MasterActor.props, "MasterActor")
  masterActor ! Read
}
