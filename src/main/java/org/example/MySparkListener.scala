package org.example

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler._
import org.apache.spark.{ExceptionFailure, TaskFailedReason, TaskKilled}

class MySparkListener extends SparkListener {
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val stageInfo = stageCompleted.stageInfo

    println(s"==== stage: ${stageInfo.name}, stage ID: ${stageInfo.stageId}")
    printf("==== submissionTime: %d\n==== completionTime: %d\n",
      stageInfo.submissionTime.getOrElse(0), stageInfo.completionTime.getOrElse(0))

    //val metrics = stageInfo.taskMetrics
    //printTaskMetrics(metrics)
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): scala.Unit = {
    val metrics = taskEnd.taskMetrics
    printTaskMetrics(metrics)

    printTaskErrMsg(taskEnd)
  }

  private def printTaskErrMsg(taskEnd: SparkListenerTaskEnd): Unit = {
    println(s"==== Task status: ${taskEnd.reason}")
    val errMsg: Option[String] =
      taskEnd.reason match {
        case kill: TaskKilled =>
          Some(kill.toErrorString)
        case e: ExceptionFailure =>
          Some(e.toErrorString)
        case e: TaskFailedReason =>
          Some(e.toErrorString)
        case _ => None
      }
    if (errMsg.nonEmpty) {
      println(s"==== Task error: $errMsg")
    }
  }

  private def printTaskMetrics(metrics: TaskMetrics): Unit = {
    println(s"==== executorCpuTime: ${metrics.executorCpuTime}\n" +
        s"==== executorRunTime: ${metrics.executorRunTime}\n" +
        s"==== executorDeserializeCpuTime: ${metrics.executorDeserializeCpuTime}\n" +
        s"==== executorDeserializeTime: ${metrics.executorDeserializeTime}\n" +
        s"==== resultSerializationTime: ${metrics.resultSerializationTime}\n" +
        s"==== jvmGCTime: ${metrics.jvmGCTime}\n" +
        s"==== peakExecutionMemory: ${metrics.peakExecutionMemory}"
    )
    if (metrics.inputMetrics != null) {
      println(s"==== recordsRead: ${metrics.inputMetrics.recordsRead}\n" +
          s"==== bytesRead: ${metrics.inputMetrics.bytesRead}")
    }
    if (metrics.outputMetrics != null) {
      println(s"==== recordsWritten: ${metrics.outputMetrics.recordsWritten}\n" +
          s"==== bytesWritten: ${metrics.outputMetrics.bytesWritten}")
    }
    if (metrics.shuffleReadMetrics != null) {
      println(s"==== shuffle fetchWaitTime: ${metrics.shuffleReadMetrics.fetchWaitTime}\n" +
          s"==== shuffle recordsRead: ${metrics.shuffleReadMetrics.recordsRead}\n" +
          s"==== shuffle totalBytesRead: ${metrics.shuffleReadMetrics.totalBytesRead}")
    }
    if (metrics.shuffleWriteMetrics != null) {
      println(s"==== shuffle recordsWritten: ${metrics.shuffleWriteMetrics.recordsWritten}\n" +
          s"==== shuffle bytesWritten: ${metrics.shuffleWriteMetrics.bytesWritten}")
    }
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {}

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {}

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = {}

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {}

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {}

  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = {}

  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = {}

  override def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = {}

  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit = {}

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {}

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {}

  override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = {}

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {}

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {}

  override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit = {}

  override def onOtherEvent(event: SparkListenerEvent): Unit = {}
}
