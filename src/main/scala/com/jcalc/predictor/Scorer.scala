package com.jcalc.predictor

import scala.collection.mutable.ArrayBuffer

trait Scorer {
  def calcScore(stateSeq: Array[String], startingIndex: Int): Double
}

object Scorer {

  private class MissProbability(stateTranstionProb: Array[Array[Double]], states: Array[String], stateSeqWindowSize: Int) extends Scorer {

    override def calcScore(stateSeq: Array[String], startingIndex: Int): Double = {
      val endingIndex = startingIndex + stateSeqWindowSize - 2
      var score = 0.0
      for (i <- startingIndex to endingIndex) { // pre inc i
        val currentState = states.indexOf(stateSeq(i));
        val nextState = states.indexOf(stateSeq(i + 1));

        //add all probability except target state
        println("states size =  " + states.size)
        for (j <- 0 to states.size - 1) { // pre inc j
          println("j=" + j)
          if (j != nextState) {
             score += stateTranstionProb(currentState)(j);
          }
        }

      }
      score

    }
  }

  private class MissRate(stateTranstionProb: Array[Array[Double]], states: Array[String], stateSeqWindowSize: Int) extends Scorer {
    val numberStates = states.length
    val maxStateProbIndex = new Array[Int](numberStates);
    for (i <- 0 to numberStates - 1) {
      var maxProbIndex = -1;
      var maxProb = -1.0;
      for (j <- 0 to numberStates - 1) {
        if (stateTranstionProb(i)(j) > maxProb) {
          maxProb = stateTranstionProb(i)(j);
          maxProbIndex = j;
        }
      }
      maxStateProbIndex(i) = maxProbIndex;
    }

    override def calcScore(stateSeq: Array[String], startingIndex: Int): Double = {
      val endingIndex = startingIndex + stateSeqWindowSize - 2
      var score = 0.0

      for (i <- startingIndex to endingIndex) {
        val currentState = states.indexOf(stateSeq(i));
        val nextState = states.indexOf(stateSeq(i + 1));
        score += (if (nextState == maxStateProbIndex(currentState)) 0.0 else 1.0);
      }
//      println("final score = " + score)
      score
    }
  }

  private class entropyReduction(stateTranstionProb: Array[Array[Double]], states: Array[String], stateSeqWindowSize: Int) extends Scorer {
    val numberStates = states.length
    val entropy = new Array[Double](numberStates);
    for (i <- 0 to numberStates - 1) {
      var ent = 0.0;
      for (j <- 0 to numberStates - 1) {
        ent += -stateTranstionProb(i)(j) * Math.log(stateTranstionProb(i)(j));
      }
      entropy(i) = ent;
    }

    override def calcScore(stateSeq: Array[String], startingIndex: Int): Double = {
      val endingIndex = startingIndex + stateSeqWindowSize - 2
      val start = 0;
      var entropyWoTargetState = 0.0;
      var entropy = 0.0;

      for (i <- start to endingIndex) {
        val currentState = states.indexOf(stateSeq(i));
        val nextState = states.indexOf(stateSeq(i + 1));

        for (j <- 0 to states.length - 1) {
          val current = stateTranstionProb(currentState)(j);
          val entIncr = -current * Math.log(current);

          //entropy without target state
          if (j != nextState) {
            entropyWoTargetState += entIncr;
          }

          //full entropy
          entropy += entIncr;
        }
      }
      val score = entropyWoTargetState / entropy;
      score
    }
  }

  // our 'factory' method
  def apply(s: String, stateTranstionProb: Array[Array[Double]], states: Array[String], stateSeqWindowSize: Int): Scorer = {
    if (s == "MissProbability") {
      return new MissProbability(stateTranstionProb, states, stateSeqWindowSize)
    } else if (s == "MissRate") {
      return new MissRate(stateTranstionProb, states, stateSeqWindowSize)
    } else {
      return new entropyReduction(stateTranstionProb, states, stateSeqWindowSize)
    }
  }

}