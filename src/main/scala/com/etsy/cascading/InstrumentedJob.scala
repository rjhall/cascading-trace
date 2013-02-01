package com.etsy.cascading

import com.twitter.scalding.{Args, Job, Mode, Source, Write}

class TracingJob(args : Args) extends Job(args) {

  val instrument = new InputTracingInstrumentation()

  def trace(src : Source, dst : Source) : Source = {
    instrument.registerTracing(src.toString, dst.createTap(Write))
    src
  }
  
  override def buildFlow(implicit mode : Mode) = { 
    validateSources(mode)
    val instrumenter = new FlowInstrumenter(instrument)
    val fd = instrumenter.instrument(flowDef)
    mode.newFlowConnector(config).connect(fd)
  }
}
