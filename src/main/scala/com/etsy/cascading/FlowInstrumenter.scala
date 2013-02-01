package com.etsy.cascading

import scala.collection.JavaConverters._

import cascading.flow.FlowDef
import cascading.operation.Identity
import cascading.pipe.{CoGroup, Each, Every, GroupBy, HashJoin, Pipe}
import cascading.pipe.assembly.AggregateBy
import cascading.tap.Tap
import cascading.tuple.{Fields,Tuple,TupleEntry}

import com.twitter.algebird.{BF, BloomFilterMonoid, Monoid}
import com.twitter.algebird.Operators._

import com.twitter.scalding.{MRMAggregator, MRMBy, RichPipe, Source, TupleConverter, TupleSetter, Write}

class FlowInstrumenter[T](instrumentation : FlowInstrumentation[T])(implicit tconv : TupleConverter[T], tsetter : TupleSetter[T])  extends Serializable {

  import com.twitter.scalding.Dsl._
  
  var pipe_map : Map[Pipe, Pipe] = Map[Pipe,Pipe]()
  var head : List[Pipe] = List[Pipe]()
  val field = new Fields(instrumentation.instrumentationFieldName)
  val field2 = new Fields(instrumentation.instrumentationFieldName + "2")

  def instrument(flowDef : FlowDef) : FlowDef = {
    println("orig flow:")
    print_flow(flowDef.getTails.asScala.head)
    // Build new flow starting with tails.
    val tails = flowDef.getTails.asScala.map{ recurseUp(_) }.toList
    // Add stages to post process the tail contents.
    val addtails  = instrumentation.complete(tails.toList)
    // Hook up tail pipes to sinks.
    val fd = new FlowDef()
    flowDef.getTails.asScala.foreach{ p : Pipe =>
      val q = RichPipe(pipe_map(p)).discard(field)
      fd.addTail(q)
      fd.addSink(q.getName, flowDef.getSinks.get(q.getName))
      println(q.toString + " -> " + flowDef.getSinks.get(q.getName).toString)
    }
    // Connect instrumented tails to taps.
    addtails.foreach{ x : (Pipe, Tap[_,_,_]) => 
      val q = new Pipe(x._2.toString, x._1)
      fd.addTail(q); fd.addSink(x._2.toString, x._2)
      println("tracing: " + q + " -> " + x._2.toString)
    }
    // Connect head taps.
    head.foreach{ p : Pipe => 
      fd.addSource(p.getName, flowDef.getSources.get(p.getName))
      println("head: " + p.toString + " -> " + flowDef.getSources.get(p.getName))
    }
    // Return new flow def.
    println("new flow:")
    print_flow(fd.getTails.asScala.head)
    fd 
  }

  // Do surgery to instrument the flow for tracing.
  def recurseUp(pipe : Pipe) : Pipe = {
    implicit def p2rp(p : Pipe) : RichPipe = RichPipe(p)
    if(pipe_map.contains(pipe)) {
      println("reached " + pipe.toString + " again")
      pipe_map(pipe)
    } else {
      val prevs = pipe.getPrevious
      if(prevs.length == 0) {
        // Head pipe.
        println("head pipe with name: " + pipe.getName)
        // Reuse the same head pipe, but add a stage which makes the bloom filter..
        val n = pipe.map(Fields.ALL -> field)(instrumentation.create(pipe.getName))
        instrumentation.register(pipe.getName, pipe)
        head ::= pipe
        pipe_map += (pipe -> n)
        n
      } else {
        val pnew : Pipe = pipe match {
          case p : Each => {
            if(p.isFunction) {
              if(p.getOutputSelector == Fields.SWAP) {
                new Each(recurseUp(prevs.head), p.getArgumentSelector, p.getFunction, p.getOutputSelector)
              } else if(p.getFunction.isInstanceOf[Identity]) {
                val f = p.getArgumentSelector.append(field)
                new Each(recurseUp(prevs.head), f, new Identity(f))
              } else {
                // Just ensure tracing field is preserved.
                val outs = p.getOutputSelector
                val outn = if(outs == Fields.RESULTS || (outs != Fields.ALL && outs != Fields.REPLACE)) p.getFunction.getFieldDeclaration.append(field) else outs
                new Each(recurseUp(prevs.head), p.getArgumentSelector, p.getFunction, outn)
              }
            } else if(p.isFilter) {
              new Each(recurseUp(prevs.head), p.getArgumentSelector, p.getFilter)
            } else {
              throw new java.lang.Exception("unknown pipe: " + p.toString)
            }
          }
          case p : Every => {
            // Everys and groupBys that are scheduled by the AggregateBy are handled in that guys clause. 
            var parent = recurseUp(prevs.head)
            if(parent.isInstanceOf[GroupBy]) {
              // Add on a thing to aggregate.
              parent = new Every(parent, field, new MRMAggregator[T,T,T]({bf => bf}, {(a,b) => instrumentation.merge(a,b)}, {bf => bf}, field, tconv, tsetter))
            }
            if(p.isAggregator) {
              new Every(parent, p.getArgumentSelector, p.getAggregator)
            } else if(p.isBuffer) {
              new Every(parent, p.getArgumentSelector, p.getBuffer)
            } else {
              throw new java.lang.Exception("unknown pipe: " + p.toString)
            }
          }
          case p : HashJoin => {
            // TODO: self joins.
            // Rename the tracing field on one side of the input, perform cogroup, then merge fields.
            if(prevs.size == 2) {
              val left = recurseUp(prevs.head)
              val right = recurseUp(prevs.tail.head).rename(field -> field2)
              val cg = new HashJoin(left, p.getKeySelectors.get(left.getName), right, p.getKeySelectors.get(right.getName), p.getJoiner) 
              cg.map(field.append(field2) -> field){ x : (T, T) => if(x._1 == null) x._2 else if(x._2 == null) x._1 else instrumentation.merge(x._1,x._2) }
            } else {
              throw new java.lang.Exception("not yet implemented: " + p.toString + " with " + prevs.size + " parents")
            }
          }
          case p : CoGroup => {
            // TODO: self joins.
            // Rename the tracing field on one side of the input, perform cogroup, then merge fields.
            if(prevs.size == 2) {
              val left = recurseUp(prevs.head)
              val right = recurseUp(prevs.tail.head).rename(field -> field2)
              val cg = new CoGroup(left, p.getKeySelectors.get(left.getName), right, p.getKeySelectors.get(right.getName), p.getJoiner) 
              cg.map(field.append(field2) -> field){ x : (T, T) => if(x._1 == null) x._2 else if(x._2 == null) x._1 else instrumentation.merge(x._1,x._2) }
            } else {
              throw new java.lang.Exception("not yet implemented: " + p.toString + " with " + prevs.size + " parents")
            }
          }
          // TODO: set mapred.reduce.tasks on the groupBy (and the aggregateBy.getGroupBy)
          case p : GroupBy => {
            val groupfields = p.getKeySelectors.asScala.head._2
            if(p.isSorted) {
              new GroupBy(p.getName, recurseUp(prevs.head), groupfields, p.getSortingSelectors.asScala.head._2, p.isSortReversed)
            } else {
              new GroupBy(p.getName, recurseUp(prevs.head), groupfields)
            }
          }
          case p : AggregateBy => {
            // If a groupBys doing the aggregation we can get on board with that and save time.
            // Skip the groupby and the each that preceeds it.
            val inp = recurseUp(p.getGroupBy.getPrevious.head.getPrevious.head) // Skip the "Each" it makes.
            val groupfields = p.getGroupBy.getKeySelectors.asScala.head._2
            val mrm = new MRMBy[T,T,T](field, '__mid_source_data, field, 
              {bf => bf}, {(a,b) => instrumentation.merge(a,b)}, {bf => bf}, tconv, tsetter, tconv, tsetter)
            val thresholdfield = classOf[AggregateBy].getDeclaredField("threshold") // pwn
            thresholdfield.setAccessible(true)
            new AggregateBy(p.getName, inp, groupfields, thresholdfield.getInt(p), p, mrm)
          }
          case p : Pipe => {
            new Pipe(p.getName, recurseUp(prevs.head))
          }
        }
        pipe_map += (pipe -> pnew)
        pnew     
      }
    }
  }

  def print_flow(i : Int, tail : Pipe) {
    if(tail.getPrevious.size == 1) {
      print_flow(i, tail.getPrevious.apply(0))
    } else {
      (0 until tail.getPrevious.size).foreach{ j : Int =>
        print_flow(2*i + j + 1, tail.getPrevious.apply(j))
      }
    }
    println(i + " -- " + tail.toString)
  }
  
  def print_flow(tail : Pipe) {
    print_flow(0, tail)
  }

}
