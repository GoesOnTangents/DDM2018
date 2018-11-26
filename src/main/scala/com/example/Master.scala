package com.example


import akka.actor.{Actor, ActorRef, PoisonPill, Props}
import com.example.SlaveActor._

import scala.util.control.Breaks.{break, breakable}

object MasterActor {
  final val props: Props = Props(new MasterActor())
  final case class Read(filename: String)
  final case class SetSlaves(slaves: Int)

  final case object SlaveSubscription

  final case object CrackPasswords
  final case class PasswordFound(index: Int, password: Int)

  final case object SolveLinearCombination
  final case class LinearCombinationFound(combination: Long)

  final case object SolveLCS
  final case class LCSFound(index: Int, partner: Int, length: Int)
  final case object LCSFinished

  final case object MineHashes
  final case object HashMiningWorkRequest

  final case object OutputResults
}

class MasterActor extends Actor {

  import MasterActor._
  var expectedSlaveAmount: Int = 2
  var slaves: Array[ActorRef] = Array()
  var t1: Long = 0
  var t2: Long = 0

  //inputs
  var names: Array[String] = Array()
  var hashes: Array[String] = Array()
  var genes: Array[String] = Array()

  //results
  var cracked_passwords: Array[Int] = Array()
  var lcs_partner: Array[Int] = Array()
  var linear_combination: Array[Boolean] = Array()
  var mined_hashes: Array[String] = Array()

  //counter variables
  var num_cracked_passwords = 0
  var linear_combination_found = false
  var lcs_candidates_checked: Array[Int] = Array()
  var lcs_max: Array[Int] = Array()
  var lcs_students_finished: Int = 0
  var next_hash_mining_student_id = 0
  var num_hashes_stored = 0

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
      }
    }
    val num_lines = names.length
    cracked_passwords = Array.ofDim(num_lines)
    linear_combination = Array.ofDim(num_lines)
    lcs_partner = Array.ofDim(num_lines)
    lcs_candidates_checked = Array.ofDim(num_lines)
    lcs_max = Array.ofDim(num_lines)
    mined_hashes = Array.ofDim(num_lines)
  }

  def set_slave_amount(num: Int): Unit ={
    this.expectedSlaveAmount = num
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
      this.store_lcs(index, partner, length)
    case LCSFinished =>
      this.count_lcs_finishes()
    case MineHashes =>
      this.delegate_hash_mining
    case HashMiningWorkRequest =>
      this.distribute_hash_mining_work
    case HashFound(id, hash) =>
      this.store_hash(id, hash)
    case Read(filename) =>
      this.read(filename)
    case SetSlaves(slaves) =>
      this.set_slave_amount(slaves)
    case OutputResults =>
      this.print_results
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
      self ! SolveLCS
      //self ! CrackPasswords
    }
  }

  def store_password(id: Int, password: Int): Unit = {
    print(".")
    cracked_passwords(id) = password
    num_cracked_passwords += 1

    if (num_cracked_passwords == cracked_passwords.length) {
      this.t2 = System.currentTimeMillis()
      println(s"\nCracked Passwords:\n ${cracked_passwords.deep.mkString(",")},\n Total time needed: ${(this.t2-this.t1)}")
      self ! SolveLinearCombination
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
    self ! SolveLCS
  }

  def delegate_lcs(): Unit = {
    val total_lcs_comparisons = this.names.length*this.names.length
    println(s"Length of our list is: ${this.names.length} squared: $total_lcs_comparisons.\n")
    val ranges = PasswordWorker.range_split(0, total_lcs_comparisons, slaves.length)
    for (i<- slaves.indices) {
      slaves(i) ! FindLCSInRange(genes, ranges(i)._1, ranges(i)._2)
    }

  }

  def store_lcs(index: Int, partner: Int, length: Int): Unit = {
    if (lcs_max(index) < length){
      lcs_partner(index) = partner
      lcs_max(index) = length
    }
    lcs_candidates_checked(index) += 1
    if (lcs_candidates_checked(index) == genes.length - 1) {
      //print(s"Student ${index} has had ${lcs_candidates_checked(index) + 1} candidates checked. LCS: ${lcs_max(index)}, with candidate ${lcs_partner(index)}.")
      print(s"$index,")
      self ! LCSFinished
    }
  }

  def count_lcs_finishes(): Unit = {
    lcs_students_finished += 1
    if (lcs_students_finished == genes.length) {
      this.t2 = System.currentTimeMillis()
      for (i <- genes.indices) {
        val id = i + 1
        val name = names(i)
        val password = cracked_passwords(i)
        val prefix = if (linear_combination(i)) 1 else -1
        val partner = lcs_partner(i)+1 // +1? Yes. :D
        println(s"\n$id;$name;$password;$prefix;$partner;")
      }
      println(s"\n Finished LCS. \n Total time: ${(this.t2-this.t1)}")
      self ! MineHashes
    }
  }



  def delegate_hash_mining() : Unit = {
    println("Delegating hash mining.")
    for (slave <- slaves) {
      slave ! SlaveActor.MineHashes(linear_combination, lcs_partner)
    }
  }

  def distribute_hash_mining_work() : Unit = {
    if (next_hash_mining_student_id >= linear_combination.length) {
      println(s"already gave out all hash mining work packages, so ignoring request for a new one by ${sender()}")
      return
    }
    sender() ! HashMiningWorkPackage(next_hash_mining_student_id)
    next_hash_mining_student_id += 1
  }

  def store_hash(student_id: Int, hash: String) : Unit = {
    println(s"Storing hash $hash for id $student_id")
    mined_hashes(student_id) = hash
    num_hashes_stored += 1

    if (num_hashes_stored == mined_hashes.length) {
      this.t2 = System.currentTimeMillis()
      println(s"\n Found all hashes. \n Total time: ${(this.t2-this.t1)}")
      for (slave <- slaves) {
        println(s"sending poison pill to $slave")
        slave ! PoisonPill
      }

      println("Starting output phase")
      self ! OutputResults
    }
  }

  def print_results(): Unit = {
    println(s"After ${(this.t2-this.t1)} milliseconds, these are the results:")
    for (i <- genes.indices) {
      val id = i + 1
      val name = names(i)
      val password = cracked_passwords(i)
      val prefix = if (linear_combination(i)) 1 else -1
      val partner = lcs_partner(i)+1 // +1? Yes. :D
      val hash = mined_hashes(i)
      println(s"$id;$name;$password;$prefix;$partner;$hash")
    }
    println("Printing results completed. Have a nice day.")
  }
}
