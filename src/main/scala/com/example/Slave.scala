package com.example

import akka.actor.{Actor, ActorRef, Props}

import com.example.MasterActor._
import com.example.SlaveActor.{HashFound, NoLinearCombinationFound}

import scala.util.control.Breaks.{break, breakable}

object HashMiningActor {
  final val props: Props = Props(new HashMiningActor())
  final case class Setup(master_actor_adress: String, linear_combination : Array[Boolean], lcs_partner: Array[Int])
  final case class Start(student_id: Int, range: Tuple2[Long,Long])

  var current_student_id: Int = -1
}

class HashMiningActor extends Actor {
  import HashMiningActor._

  //default
  var master_actor_address: String = "akka.tcp://MasterSystem@127.0.0.1:42000/user/MasterActor"
  var linear_combination : Array[Boolean] = Array[Boolean]()
  var lcs_partner : Array[Int] = Array[Int]()

  override def receive: Receive = {
    case Setup(master_actor_address, linear_combination, lcs_partner) =>
      this.setup(master_actor_address, linear_combination, lcs_partner)
    case Start(id, range) =>
      this.mine_hashes(id, range)
  }

  def mine_hashes(student_id: Int, range: Tuple2[Long,Long]): Unit = {
    println(s"${this} started to mine hashes in range ${range._1} to ${range._2}")

    val target_prefix = if (linear_combination(student_id)) "11111" else "00000"
    val partner_id = lcs_partner(student_id)

    val min = range._1
    val max = range._2

    var nonce = 0
    var hash = ""
    val rand = new scala.util.Random
    var counter = 0
    while (!hash.startsWith(target_prefix)) {

      nonce = rand.nextInt();//(min + rand.nextInt((max - min + 1).toInt)).toInt
      hash = PasswordWorker.hash_int(partner_id + nonce)
      counter += 1
      if (counter % 100000 == 0) {
        if (current_student_id != student_id) return
      }
    }

    //password found
    println(s"found hash $hash for student with (0-based) id $student_id")
    context.parent ! HashFound(student_id, hash)
  }

  def setup(master_actor_address: String, linear_combination : Array[Boolean], lcs_partner: Array[Int]): Unit = {
    this.master_actor_address = master_actor_address
    this.linear_combination = linear_combination
    this.lcs_partner = lcs_partner
  }
}


object LCSWorker {
  final val props: Props = Props(new LCSWorker())
  final case class Start(i: Int, j: Int)
  final case class Setup(MasterAddress: String, genes: Array[String])
}

class LCSWorker extends Actor {
  import LCSWorker._

  //default
  var masterActorAddress: String = "akka.tcp://MasterSystem@127.0.0.1:42000/user/MasterActor"
  val masterActor = context.actorSelection(this.masterActorAddress)
  var genes: Array[String] = Array()

  override def receive: Receive = {
    case Start(i,j) =>
      this.make_comparisons_for_range(i,j)
    case Setup(masterAddress, genes) =>
      this.setup(masterAddress, genes)
  }

  def make_comparisons_for_range(i: Int, j: Int): Unit ={
    println(s"$this has range: ($i, $j)")
    val genes_length = genes.length
    var student: Int = i / genes_length
    var candidate: Int = i % genes_length
    var res: Int = 0
    var counter: Int = 0

    for (x <- i until j+1) {
      if (genes(student) != genes(candidate)) {
        res = lcs(genes(student), genes(candidate))
        masterActor ! LCSFound(student, candidate, res)
        counter += 1
      }
      candidate += 1
      //if all candidates for student have been exhausted, but we still got range, check the next student
      if (candidate == genes_length) {
        if (student+1 < genes_length) student += 1
        //Counting students is some circle of hell, I'm sure.
        candidate = 0
      }
    }
    //close this worker
    println(s"$this has finished after sending $counter messages to master.")
    context.stop(self)
  }

  def lcs(s: String, t: String): Int = {
    if (s.isEmpty || t.isEmpty) return 0
    val m = s.length
    val n = t.length
    var cost = 0
    var maxLen = 0
    var p = new Array[Int](n)
    var d = new Array[Int](n)
    var i = 0
    while ( {
      i < m
    }) {
      var j = 0
      while ( {
        j < n
      }) { // calculate cost/score
        if (s.charAt(i) != t.charAt(j)) cost = 0
        else if ((i == 0) || (j == 0)) cost = 1
        else cost = p(j - 1) + 1
        d(j) = cost
        if (cost > maxLen) maxLen = cost
          // for {}
        //{
          j += 1; j
        //}
      }
      val swap = p
      p = d
      d = swap

      {
        i += 1; i
      }
    }
    maxLen
  }

  def setup(masterAddress: String, gene_list: Array[String]): Unit ={
    this.masterActorAddress = masterAddress
    this.genes = gene_list
  }
}

object LinearCombinationWorker {
  final val props: Props = Props(new LinearCombinationWorker())
  final case class Start(range: Tuple2[Long,Long])
  final case class Setup(masterAddress: String, passwords: Array[Int], target_sum: Long)

  //all values inclusive
  def range_split(min: Long, max: Long, num_batches: Int): Array[Tuple2[Long,Long]] = {
    var result = Array[Tuple2[Long,Long]]()

    val total_range : Long = max - min + 1L

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

    context.parent ! NoLinearCombinationFound
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

  def hash(s: String): String = {
    val m = java.security.MessageDigest.getInstance("SHA-256").digest(s.getBytes("UTF-8"))
    m.map("%02x".format(_)).mkString
  }

  def hash_int(i: Int): String = {
    hash(i.toString)
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

  override def receive: Receive = {
    case Start(passwords, i, j) =>
      this.crack_passwords_in_range(passwords,i,j)
    case Setup(masterAddress) =>
      this.setup(masterAddress)
  }
}

object SlaveActor {
  final val props: Props = Props(new SlaveActor())

  final case class SetWorkers(num: Int)
  final case class Subscribe(addr: String)
  final case class FindLCSInRange(genes: Array[String], i: Int , j: Int)

  final case class CrackPasswordsInRange(passwords: Array[String], i: Int, j: Int)

  final case class SolveLinearCombinationInRange(passwords: Array[Int], min: Long, max: Long, target_sum: Long)
  final case object StopSolvingLinearCombination
  final case object NoLinearCombinationFound

  final case class MineHashes(linear_combination: Array[Boolean], lcs_partner: Array[Int])
  final case class HashMiningWorkPackage(student_id: Int)
  final case class HashFound(student_id: Int, hash: String)
}

class SlaveActor extends Actor {
  import SlaveActor._
  var master_actor_address: String = ""
  var num_local_workers = 2

  var linear_combination_ranges : Array[Tuple2[Long,Long]] = Array[Tuple2[Long,Long]]()
  var linear_combination_next_index = 0
  var linear_combination_actors : Array[ActorRef] = Array[ActorRef]()

  var hash_mining_actors : Array[ActorRef] = Array[ActorRef]()

  override def receive: Receive = {
    case SetWorkers(num) =>
      this.set_worker_amount(num)
    case Subscribe(addr) =>
      this.subscribe(addr)
    case CrackPasswordsInRange(passwords, i, j) =>
      this.start_password_workers(passwords, i, j)
    case SolveLinearCombinationInRange(passwords, i, j, sum) =>
      this.start_linear_combination_workers(passwords, i, j, sum)
    case FindLCSInRange(genes, i, j) =>
      this.start_lcs_workers(genes,i,j)
    case NoLinearCombinationFound =>
      this.distribute_linear_combination_work_package
    case StopSolvingLinearCombination =>
      this.stop_solving_linear_combination
    case MineHashes(linear_combination, lcs_partner) =>
      this.start_hash_mining_workers(linear_combination, lcs_partner)
    case HashMiningWorkPackage(id) =>
      this.distribute_hash_mining_work_package(id)
    case HashFound(id, hash) =>
      this.report_hash(id, hash)
  }

  def set_worker_amount(i: Int): Unit ={
    this.num_local_workers = i
  }
  def subscribe(addr: String) = {
    this.master_actor_address = addr
    val selection = context.actorSelection(addr)
    selection ! SlaveSubscription
  }

  def start_password_workers(passwords: Array[String], min: Int, max: Int): Unit = {
    import PasswordWorker.{Start,Setup}

    val ranges = PasswordWorker.range_split(min, max, num_local_workers)
    for (i <- ranges.indices) {
      val worker = context.actorOf(PasswordWorker.props, "PasswordCrackerWorker" + i)
      worker ! Setup(master_actor_address)
      worker ! Start(passwords, ranges(i)._1,ranges(i)._2)
    }
  }

  def start_linear_combination_workers(passwords: Array[Int], min: Long, max: Long, target_sum: Long): Unit = {
    import LinearCombinationWorker.{Setup, Start}

    //we want to get ranges of a fixed size, so that workers dont get stuck working on too huge datasets
    //and can abort faster if they receive the corresponding message
    //NOTE: in theory, the line below could fail if the second parameter is bigger than Int.maxValue
    //NOTE: however, this won't happen in our scenario, so we ignore it for now
    val num_ranges: Int = math.max(num_local_workers, ((max - min + 1L) / 10000000L).toInt)
    linear_combination_ranges = LinearCombinationWorker.range_split(min, max, num_ranges)
    linear_combination_actors = Array[ActorRef]()
    for (i <- 0 until num_local_workers) {
      val actor = context.actorOf(LinearCombinationWorker.props, "LinearCombinationWorker" + i)
      actor ! Setup(master_actor_address, passwords, target_sum)
      actor ! Start(linear_combination_ranges(i))
      linear_combination_actors = linear_combination_actors :+ actor
    }
    linear_combination_next_index = num_local_workers
  }

  def start_lcs_workers(genes: Array[String],i: Int,j: Int): Unit = {
    import LCSWorker.{Setup, Start}
    val ranges = PasswordWorker.range_split(i, j, num_local_workers)
    for (w <- ranges.indices) {
      val worker = context.actorOf(LCSWorker.props, "LCSWorker" + w)
      worker ! Setup(master_actor_address, genes)
      worker ! Start(ranges(w)._1, ranges(w)._2)
    }
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
      context.stop(actor)
    }
  }

  def start_hash_mining_workers(linear_combination: Array[Boolean], lcs_partner: Array[Int]) : Unit = {
    import HashMiningActor.Setup

    for (i <- 0 until num_local_workers) {
      val actor: ActorRef = context.actorOf(HashMiningActor.props)
      actor ! Setup(master_actor_address, linear_combination, lcs_partner)
      hash_mining_actors = hash_mining_actors :+ actor
    }

    val master_actor = context.actorSelection(master_actor_address)
    master_actor ! HashMiningWorkRequest
  }

  def distribute_hash_mining_work_package(student_id: Int) : Unit = {
    import HashMiningActor.Start

    HashMiningActor.current_student_id = student_id

    val hash_mining_ranges : Array[Tuple2[Long,Long]] = LinearCombinationWorker.range_split(Int.MinValue + 2, Int.MaxValue - 2, num_local_workers)
    for (i <- hash_mining_actors.indices) {
      hash_mining_actors(i) ! Start(student_id, hash_mining_ranges(i))
    }
  }

  def report_hash(student_id: Int, hash: String): Unit = {
    //NOTE: We assume that we do not get 2 HashMiningWorkPackage messages without getting a HashFound message in between
    //NOTE: This should be a valid assumption as long as we control the master, however dunno if its good style
    if (student_id != HashMiningActor.current_student_id) {
      println(s"${this.sender()} found a hash for student with id $student_id. Discarding it as we already found a hash for that id.")
      return
    }

    val master_actor = context.actorSelection(master_actor_address)
    HashMiningActor.current_student_id = -1 //invalid value, causing second/third etc HashFound messages to be ignored
    master_actor ! HashFound(student_id, hash)
    master_actor ! HashMiningWorkRequest
  }

  override def postStop(): Unit = {
    super.postStop()
    context.system.terminate()
  }
}
