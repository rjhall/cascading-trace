package com.etsy.cascading

import cascading.pipe.Pipe
import cascading.tap.Tap
import cascading.tuple.{Fields, TupleEntry}

import com.twitter.algebird.{BF, BloomFilterMonoid, Monoid}
import com.twitter.scalding.{RichPipe, Source}

abstract class FlowInstrumentation[T] extends Serializable {

  // Let the instrumentation see the source taps and head pipes.
  protected var head_pipes = Map[String,Pipe]()

  def register(tapname : String, head : Pipe) : Unit = {
    head_pipes += (tapname -> head)
  }

  // Gives a function to create the instrumentation for a particular
  // head tap.
  def create(tapname : String) : TupleEntry => T

  // The operation to be performed during a groupBy, or a join.
  def merge(a : T, b : T) : T

  // The operations to be performed on the tail pipes at the end.
  def complete(pipe : Traversable[Pipe]) : Map[Pipe,Tap[_,_,_]]

  def instrumentationFieldName = "__instrumentation_field__"
}

class InputTracingInstrumentation extends FlowInstrumentation[Map[String,BF]] {

  implicit val bfm = new BloomFilterMonoid(5, 1 << 24, 0)
  protected var sinks = Map[String,Tap[_,_,_]]()

  def registerTracing(headtapname : String, output : Tap[_,_,_]) : Unit = {
    sinks += (headtapname -> output)
  }

  def create(tapname : String) : TupleEntry => Map[String,BF] = {
    if(sinks.contains(tapname)) {
      { te : TupleEntry => Map[String, BF](tapname -> bfm.create(te.getTuple.toString)) }
    } else {
      { _ => Map[String, BF](tapname -> bfm.zero) }
    }
  }

  def merge(a : Map[String,BF], b : Map[String,BF]) : Map[String,BF] = {
    implicitly[Monoid[Map[String,BF]]].plus(a,b)
  }

  def complete(tails : Traversable[Pipe]) : Map[Pipe,Tap[_,_,_]] = {
    import com.twitter.scalding.Dsl._
    var bfp = Map[String,Pipe]()
    tails.foreach{ p : Pipe =>
      sinks.map{ _._1 }.foreach{ s : String => 
        val q = p.mapTo(instrumentationFieldName -> '__bf){ m : Map[String,BF] => m(s) }.groupAll{ _.plus[BF]('__bf) }
        if(bfp.contains(s)) {
          bfp += (s -> q.rename('__bf -> '__bf2).crossWithTiny(bfp(s)).mapTo(('__bf, '__bf2) -> '__bf) { x : (BF, BF) => x._1 ++ x._2 })
        } else {
          bfp += (s -> q)
        }
      }
    }
    bfp.map{ sp : (String, Pipe)  => 
      val pipe = head_pipes(sp._1).map(Fields.ALL -> '__str){ te : TupleEntry => te.getTuple.toString }
                  .crossWithTiny(sp._2).filter('__str, '__bf){ x : (String, BF) => x._2.contains(x._1).isTrue }
                  .discard('__str, '__bf)
      val tap = sinks(sp._1)
      (pipe, tap)
    }.toMap
  }

}
