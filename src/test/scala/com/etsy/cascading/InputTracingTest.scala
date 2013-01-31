package com.etsy.cascading

import com.twitter.scalding._

import org.specs._

class InstrumentedMapJob(args : Args) extends Job(args) {
  val inp = Tsv("input", ('x,'y))
  val sm = Map(inp.asInstanceOf[Source] -> Tsv("subsample").asInstanceOf[Source])
  inp.mapTo(('x, 'y) -> 'z){ x : (Int, Int) => x._1 + x._2 }
     .write(Tsv("output"))
  
  override def buildFlow(implicit mode : Mode) = {
    validateSources(mode)
    // Sources are good, now connect the flow:
    val fd = FlowInstrumentation(flowDef, sm)
    mode.newFlowConnector(config).connect(fd)
  }
}

class InstrumentedMapTest extends Specification with TupleConversions {
  import Dsl._
  "Instrumented flow with only mappers" should {
    //Set up the job:
    "work" in {
      JobTest("com.etsy.cascading.InstrumentedMapJob")
        .source(Tsv("input", ('x,'y)), List(("0","1"), ("1","3"), ("2","9")))
        .sink[(Int)](Tsv("output")) { outBuf =>
          val unordered = outBuf.toSet
          unordered.size must be_==(3)
          unordered((1)) must be_==(true)
          unordered((4)) must be_==(true)
          unordered((11)) must be_==(true)
        }
        .sink[(Int,Int)](Tsv("subsample")) { outBuf => 
          val unordered = outBuf.toSet
          unordered.size must be_==(3)
          unordered((0,1)) must be_==(true)
          unordered((1,3)) must be_==(true)
          unordered((2,9)) must be_==(true)
        }
        .runHadoop
        .finish
    }
  }
}


class InstrumentedGroupByJob(args : Args) extends Job(args) {
  val source = Tsv("input", ('x, 'y))
  source.groupBy('x){ _.sum('y -> 'y) }
    .filter('x) { x : Int => x < 2 }
    .map('y -> 'y){ y : Double => y.toInt }
    .project('x, 'y)
    .write(Tsv("output"))

  override def buildFlow(implicit mode : Mode) = {
    validateSources(mode)
    // Sources are good, now connect the flow:
    val fd = FlowInstrumentation(flowDef, Map[Source,Source](source -> Tsv("foo/input")))
    mode.newFlowConnector(config).connect(fd)
  }
}

class InstrumentedGroupByTest extends Specification with TupleConversions {
  import Dsl._
  "Instrumented flow with aggregation" should {
    //Set up the job:
    "work" in {
      JobTest("com.etsy.cascading.InstrumentedGroupByJob")
        .source(Tsv("input", ('x,'y)), List(("0","1"), ("0","3"), ("1","9"), ("1", "1"), ("2", "5"), ("2", "3"), ("3", "3")))
        .sink[(Int,Int)](Tsv("output")) { outBuf =>
          val unordered = outBuf.toSet
          unordered.size must be_==(2)
          unordered((0,4)) must be_==(true)
          unordered((1,10)) must be_==(true)
        }
        .sink[(Int,Int)](Tsv("foo/input")) { outBuf => 
          val unordered = outBuf.toSet
          unordered.size must be_==(4)
          unordered((0,1)) must be_==(true)
          unordered((0,3)) must be_==(true)
          unordered((1,1)) must be_==(true)
          unordered((1,9)) must be_==(true)
        }
        .runHadoop
        .finish
    }
  }
}


class InstrumentedGroupByNopJob(args : Args) extends Job(args) {
  val source = Tsv("input", ('x, 'y))
  source.groupBy('x){ _.reducers(1) }
    .filter('x) { x : Int => x < 2 }
    .write(Tsv("output"))

  override def buildFlow(implicit mode : Mode) = {
    validateSources(mode)
    // Sources are good, now connect the flow:
    val fd = FlowInstrumentation(flowDef, Map[Source,Source](source -> Tsv("foo/input")))
    mode.newFlowConnector(config).connect(fd)
  }
}

class InstrumentedGroupByNopTest extends Specification with TupleConversions {
  import Dsl._
  "instrumented flow with grouping and no every" should {
    //Set up the job:
    "work" in {
      JobTest("com.etsy.cascading.InstrumentedGroupByNopJob")
        .source(Tsv("input", ('x,'y)), List(("0","1"), ("0","3"), ("1","9"), ("1", "1"), ("2", "5"), ("2", "3"), ("3", "3")))
        .sink[(Int,Int)](Tsv("output")) { outBuf =>
          val unordered = outBuf.toSet
          unordered.size must be_==(4)
          unordered((0,1)) must be_==(true)
          unordered((0,3)) must be_==(true)
          unordered((1,1)) must be_==(true)
          unordered((1,9)) must be_==(true)
        }
        .sink[(Int,Int)](Tsv("foo/input")) { outBuf => 
          val unordered = outBuf.toSet
          unordered.size must be_==(4)
          unordered((0,1)) must be_==(true)
          unordered((0,3)) must be_==(true)
          unordered((1,1)) must be_==(true)
          unordered((1,9)) must be_==(true)
        }
        .runHadoop
        .finish
    }
  }
}

class InstrumentedGroupByFoldJob(args : Args) extends Job(args) {
  val source = Tsv("input", ('x, 'y))
  source.groupBy('x){ _.foldLeft[Double,Int]('y -> 'y)(0.0){ (a : Double, b : Int) => a + b } }
    .filter('x) { x : Int => x < 2 }
    .map('y -> 'y){ y : Double => y.toInt }
    .write(Tsv("output"))

  override def buildFlow(implicit mode : Mode) = {
    validateSources(mode)
    // Sources are good, now connect the flow:
    val fd = FlowInstrumentation(flowDef, Map[Source,Source](source -> Tsv("foo/input")))
    mode.newFlowConnector(config).connect(fd)
  }
}

class InstrumentedGroupByFoldTest extends Specification with TupleConversions {
  import Dsl._
  "instrumented flow with grouping and no aggregation" should {
    //Set up the job:
    "work" in {
      JobTest("com.etsy.cascading.InstrumentedGroupByFoldJob")
        .source(Tsv("input", ('x,'y)), List(("0","1"), ("0","3"), ("1","9"), ("1", "1"), ("2", "5"), ("2", "3"), ("3", "3")))
        .sink[(Int,Int)](Tsv("output")) { outBuf =>
          val unordered = outBuf.toSet
          unordered.size must be_==(2)
          unordered((0,4)) must be_==(true)
          unordered((1,10)) must be_==(true)
        }
        .sink[(Int,Int)](Tsv("foo/input")) { outBuf => 
          val unordered = outBuf.toSet
          unordered.size must be_==(4)
          unordered((0,1)) must be_==(true)
          unordered((0,3)) must be_==(true)
          unordered((1,1)) must be_==(true)
          unordered((1,9)) must be_==(true)
        }
        .runHadoop
        .finish
    }
  }
}

class InstrumentedJoinJob(args : Args) extends Job(args) {
  val source = Tsv("input", ('x, 'y))
  val source2 = Tsv("input2", ('x, 'z))
  source.joinWithSmaller('x -> 'x, source2.read)
    .project('x, 'y, 'z)
    .write(Tsv("output"))

  override def buildFlow(implicit mode : Mode) = {
    validateSources(mode)
    // Sources are good, now connect the flow:
    val fd = FlowInstrumentation(flowDef, Map[Source,Source](source -> Tsv("foo/input"), 
                                                     source2 -> Tsv("bar/input2")))
    mode.newFlowConnector(config).connect(fd)
  }
}

class InstrumentedJoinTest extends Specification with TupleConversions {
  import Dsl._
  "instrumented coGroup" should {
    //Set up the job:
    "work" in {
      JobTest("com.etsy.cascading.InstrumentedJoinJob")
        .arg("write_sources", "true")
        .source(Tsv("input", ('x,'y)), List(("0","1"), ("1","3"), ("2","9"), ("10", "0")))
        .source(Tsv("input2", ('x, 'z)), List(("5","1"), ("1","4"), ("2","7")))
        .sink[(Int,Int,Int)](Tsv("output")) { outBuf =>
          val unordered = outBuf.toSet
          unordered.size must be_==(2)
          unordered((1,3,4)) must be_==(true)
          unordered((2,9,7)) must be_==(true)
        }
        .sink[(Int,Int)](Tsv("foo/input")) { outBuf => 
          val unordered = outBuf.toSet
          unordered.size must be_==(2)
          unordered((1,3)) must be_==(true)
          unordered((2,9)) must be_==(true)
        }
        .sink[(Int,Int)](Tsv("bar/input2")) { outBuf => 
          val unordered = outBuf.toSet
          unordered.size must be_==(2)
          unordered((1,4)) must be_==(true)
          unordered((2,7)) must be_==(true)
        }
        .runHadoop
        .finish
    }
  }
}


class InstrumentedJoinTinyJob(args : Args) extends Job(args) {
  val source = Tsv("input", ('x, 'y))
  val source2 = Tsv("input2", ('x, 'z))
  source.joinWithTiny('x -> 'x, source2.read)
    .project('x, 'y, 'z)
    .write(Tsv("output"))

  override def buildFlow(implicit mode : Mode) = {
    validateSources(mode)
    // Sources are good, now connect the flow:
    val fd = FlowInstrumentation(flowDef, Map[Source,Source](source -> Tsv("foo/input"), 
                                                     source2 -> Tsv("bar/input2")))
    mode.newFlowConnector(config).connect(fd)
  }
}

class InstrumentedJoinTinyTest extends Specification with TupleConversions {
  import Dsl._
  "instrumented hashjoin" should {
    //Set up the job:
    "work" in {
      JobTest("com.etsy.cascading.InstrumentedJoinTinyJob")
        .arg("write_sources", "true")
        .source(Tsv("input", ('x,'y)), List(("0","1"), ("1","3"), ("2","9"), ("10", "0")))
        .source(Tsv("input2", ('x, 'z)), List(("5","1"), ("1","4"), ("2","7")))
        .sink[(Int,Int,Int)](Tsv("output")) { outBuf =>
          val unordered = outBuf.toSet
          unordered.size must be_==(2)
          unordered((1,3,4)) must be_==(true)
          unordered((2,9,7)) must be_==(true)
        }
        .sink[(Int,Int)](Tsv("foo/input")) { outBuf => 
          val unordered = outBuf.toSet
          unordered.size must be_==(2)
          unordered((1,3)) must be_==(true)
          unordered((2,9)) must be_==(true)
        }
        .sink[(Int,Int)](Tsv("bar/input2")) { outBuf => 
          val unordered = outBuf.toSet
          unordered.size must be_==(2)
          unordered((1,4)) must be_==(true)
          unordered((2,7)) must be_==(true)
        }
        .runHadoop
        .finish
    }
  }
}
