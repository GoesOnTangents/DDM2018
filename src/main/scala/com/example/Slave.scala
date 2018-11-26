package com.example

import akka.actor.{Actor, ActorRef, Props}

import com.example.MasterActor._
import com.example.SlaveActor.{HashFound, NoLinearCombinationFound}

import scala.util.control.Breaks.{break, breakable}

object HashMiningActor {
  final val props: Props = Props(new HashMiningActor())
  final case class Setup(master_actor_adress: String, linear_combination : Array[Boolean], lcs_partner: Array[Int])
  final case class Start(student_id: Int, range: Tuple2[Int,Int])
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

  //we consider id's as starting from 0, not 1
  //TODO: do partner ids start with 0 or 1?
  def mine_hashes(student_id: Int, range: Tuple2[Int,Int]): Unit = {
    println(s"${this} started to mine hashes in range ${range._1} to ${range._2}")

    val target_prefix = if (linear_combination(student_id)) "11111" else "00000"
    val partner_id = lcs_partner(student_id)

    val min = range._1
    val max = range._2

    var nonce = 0
    var hash = ""
    val rand = new scala.util.Random
    while (!hash.startsWith(target_prefix)) {
      nonce = min + rand.nextInt(max - min + 1)
      hash = PasswordWorker.hash_int(partner_id + nonce)
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

    //TODO: make condition less hacky and more "until a certain student + candidate combination is met
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


  def lcs(a: String, b: String): Int ={
    /*val start = 20
    val end   = 30
    val rnd = new scala.util.Random
    start + rnd.nextInt( (end - start) + 1 )
    lcsM(a.toList, b.toList).mkString.length()*/
    val ret: String = longestCommonSubstring(a,b)
    println(s"$ret")
    ret.length()
  }

  def longestCommonSubstring(s: String, t: String): String = {
    import scala.annotation.tailrec
    val p = (s.length, t.length)
    val nonEmpty = s.nonEmpty && t.nonEmpty

    @tailrec
    def iter(lcSufx: Map[(Int, Int), Int], indexes: (Int, Int), z: Int): Map[(Int, Int), Int] = {
      val (i, j) = indexes

      def newIndexes: (Int, Int) = if (j == p._2) (i + 1, 1) else (i, j + 1)

      if (indexes != p && nonEmpty)
        if (s(i - 1) == t(j - 1)) {
          val count = lcSufx.withDefaultValue(0)((i - 1, j - 1)) + 1

          @inline
          def newLcSufx = lcSufx.filter(_._2 >= z).updated(indexes, count)

          iter(newLcSufx, newIndexes, if (count >= z) count else z)
        } else iter(lcSufx, newIndexes, z)
      else lcSufx.filter(_._2 > 1)
    }

    iter(Map.empty[(Int, Int), Int], (1, 1), 0).map {
      case ((i, _), z) => s.substring(i - z, i)
    }.toSeq.mkString
  }

  def lcs_dp(s1: String, s2: String): String = {
    if (s1 == null || s1.length() == 0 || s2 == null || s2.length() == 0) ""
    else if (s1 == s2) s1
    else {
      val up = 1
      val left = 2
      val charMatched = 3

      val s1Length = s1.length()
      val s2Length = s2.length()

      val lcsLengths = Array.fill[Int](s1Length + 1, s2Length + 1)(0)

      for (i <- 0 until s1Length) {
        for (j <- 0 until s2Length) {
          if (s1.charAt(i) == s2.charAt(j)) {
            lcsLengths(i + 1)(j + 1) = lcsLengths(i)(j) + 1
          } else {
            if (lcsLengths(i)(j + 1) >= lcsLengths(i + 1)(j)) {
              lcsLengths(i + 1)(j + 1) = lcsLengths(i)(j + 1)
            } else {
              lcsLengths(i + 1)(j + 1) = lcsLengths(i + 1)(j)
            }
          }
        }
      }

      val subSeq = new StringBuilder()
      var s1Pos = s1Length
      var s2Pos = s2Length

      // build longest subsequence by backtracking
      do {
        if (lcsLengths(s1Pos)(s2Pos) == lcsLengths(s1Pos -1)(s2Pos)) {
          s1Pos -= 1
        } else if (lcsLengths(s1Pos)(s2Pos) == lcsLengths(s1Pos)(s2Pos - 1)) {
          s2Pos -= 1
        } else {
          assert(s1.charAt(s1Pos - 1) == s2.charAt(s2Pos - 1))
          subSeq += s1.charAt(s1Pos - 1)
          s1Pos -= 1
          s2Pos -= 1
        }

      } while (s1Pos > 0 && s2Pos > 0)

      subSeq.toString.reverse
    }
  }

  /*//<<------LCS MAGIC------>>\\
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
  //<<------LCS MAGIC------>>\\*/


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
  //TODO: this is a (almost-)duplicate of PasswordWorker.range_split
  //TODO: if we got too much time, we could figure out how generics work in scala and reduce redundancy
  def range_split(min: Long, max: Long, num_batches: Int): Array[Tuple2[Long,Long]] = {
    var result = Array[Tuple2[Long,Long]]()

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
  var hash_mining_actors_current_index = -1 //invalid value

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
    //TODO: in theory, the line below could fail if the second parameter is bigger than Int.maxValue
    //TODO: however, this won't happen in our scenario, so we ignore it for now
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
      context.stop(actor) //TODO good style, or rather PoisonPill?
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

    hash_mining_actors_current_index = student_id

    val hash_mining_ranges : Vector[Tuple2[Int,Int]] = PasswordWorker.range_split(Int.MinValue, Int.MaxValue, num_local_workers)
    for (i <- hash_mining_actors.indices) {
      hash_mining_actors(i) ! Start(student_id, hash_mining_ranges(i))
    }
  }

  def report_hash(student_id: Int, hash: String): Unit = {
    //TODO: We assume that we do not get 2 HashMiningWorkPackage messages without getting a HashFound message in between
    //TODO: This should be a valid assumption as long as we control the master, however dunno if its good style
    if (student_id != hash_mining_actors_current_index) {
      println(s"${this.sender()} found a hash for student with id $student_id. Discarding it as we already found a hash for that id.")
      return
    }

    val master_actor = context.actorSelection(master_actor_address)
    hash_mining_actors_current_index = -1 //invalid value, causing second/third etc HashFound messages to be ignored
    master_actor ! HashFound(student_id, hash)
  }
}
