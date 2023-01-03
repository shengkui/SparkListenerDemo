package org.example

import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

class MyQueryExecutionListener extends QueryExecutionListener {
  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    println(s"==== [${funcName}] Success")
    //println(qe.executedPlan.prettyJson)
    //    val it = qe.executedPlan.metrics.iterator
    //    while (it.hasNext) {
    //      val next = it.next
    //      printf("Key: %s, value: %s%n", next._1, next._2.value)
    //    }
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    println(s"==== [${funcName}] Failure")
    //println(qe.executedPlan.prettyJson)
  }
}
