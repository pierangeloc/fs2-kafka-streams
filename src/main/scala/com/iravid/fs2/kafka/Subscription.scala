package com.iravid.fs2.kafka

sealed trait Subscription
object Subscription {
  case class Topics(topics: List[String]) extends Subscription
  case class Pattern(pattern: String) extends Subscription
}
