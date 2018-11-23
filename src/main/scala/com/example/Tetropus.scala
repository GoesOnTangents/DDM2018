package com.example

import java.io.File
import com.example._
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}

object Tetropus extends App {
  if (args.length == 0) {
    println("dude, you didn't give me any parameters")
  }
}