package com.example

import java.io.File

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.example.MasterActor.Read
import com.example.SlaveActor.{CrackPasswordsInRange, FindLCSInRange, SolveLinearCombinationInRange}
import com.typesafe.config.ConfigFactory
import akka.actor.{Actor, ActorRef, Props}
import com.example.SlaveActor.{CrackPasswordsInRange, SolveLinearCombinationInRange, StopSolvingLinearCombination}

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
  case object SolveLCS
  case class LCSFound(index: Int, partner: Int, length: Int)
  case object LCSFinished
}

class MasterActor extends Actor {

  import MasterActor._
  var expectedSlaveAmount: Int = 2
  var slaves: Array[ActorRef] = Array()
  var t1: Long = 0
  var t2: Long = 0

  var names: Array[String] = Array()
  var hashes: Array[String] = Array()
  var genes: Array[String] = Array()

  //results
  var cracked_passwords: Array[Int] = Array()
  var lcs_partner: Array[Int] = Array()
  var linear_combination: Array[Boolean] = Array()
  var partner_hashes: Array[String] = Array()

  //counter variables
  var lcs_length: Array[Int] = Array()
  var num_cracked_passwords = 0
  var linear_combination_found = false
  var slaves_finished_with_LCS = 0

  def read(filename: String): Unit = {
    val file_contents =
      scala.io.Source.fromFile(filename).getLines().drop(1)
    breakable {
      for (line <- file_contents) {
        if (line == "") break
        val cols = line.split(";").map(_.trim)
        this.names = this.names :+ cols(1)
        this.hashes = this.hashes :+ cols(2)
        this.genes = this.genes :+ cols(3)

        //println(s"${cols(0)}|${cols(1)}|${cols(2)}|${cols(3)}")
      }
    }
    val num_lines = names.length
    cracked_passwords = Array.ofDim(num_lines)
    linear_combination = Array.ofDim(num_lines)
    lcs_partner = Array.ofDim(num_lines)
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
    case SolveLCS =>
      this.delegate_lcs()
    case LCSFound(index, partner, length) =>
      this.store_LCS(index, partner, length)
    case LCSFinished =>
      this.count_LCS_finishes()
    case Read(filename) =>
      this.read(filename)
    case msg: Any => throw new RuntimeException("unknown message type " + msg);

  }

  def delegate_password_cracking(): Unit = {
    println("Delegating Passwords to crack.")
    this.t1 = System.currentTimeMillis()
    println(s"Started cracking Passwords. t1: ${this.t1}")
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
      this.t2 = System.currentTimeMillis()
      println(s"\nCracked Passwords:\n ${cracked_passwords.deep.mkString(",")},\n Total time needed: ${(this.t2-this.t1)}")
    }
  }

  //Note: This code might fail if more than 63 datasets have to processed,
  //as we use Long (64 bit) as a bitmask to encode which passwords are part of the linear combination
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

    //tell slaves to stop working on the linear combination
    for (slave <- slaves) {
      println(s"Sending stop solving linear combination to $slave")
      slave ! StopSolvingLinearCombination
    }

    //decode linear combination
    var sum: Long = 0
    for (index <- linear_combination.indices) {
      linear_combination(index) = (combination & (1L << index)) > 0
      if (linear_combination(index)) {
        sum += cracked_passwords(index)
      }
    }
    this.t2 = System.currentTimeMillis()
    println(s"sum is $sum.\nTotal time taken: ${(this.t2-this.t1)}")
    linear_combination_found = true

    //TODO trigger next step
    println("TODO: trigger gene subsequence tasks")
  }

  def delegate_lcs(): Unit = {
    val total_lcs_calls = this.names.length*this.names.length
    println(s"Length of our list is: ${this.names.length} and we square it to: $total_lcs_calls.")
    val ranges = PasswordWorker.range_split(0, total_lcs_calls, slaves.length)
    for (i<- slaves.indices) {
      slaves(i) ! FindLCSInRange(genes, ranges(i)._1, ranges(i)._2)
    }

  }

  def store_LCS(index: Int, partner: Int, length: Int): Unit = {
    if (lcs_length(index) < length){
      lcs_partner(index) = partner
      lcs_length(index) = length
    }
  }

  def count_LCS_finishes(): Unit = {
    this.slaves_finished_with_LCS += 1
    if (this.slaves_finished_with_LCS == slaves.length) {
      //start_next_stage
      this.t2 = System.currentTimeMillis()
      println(s"Found all best LCS'. \n Total time taken: ${(this.t2-this.t1)}")
    }
  }

  def find_prefixed_hashes(): Unit = {
    // Give slaves names
    // Distribute new names when finished...
  }
}
