---
layout: post
title: "Spark Steaming算子之mapwithstate和updatebykey"
categories: "Spark"
tags: [SparkStreaming]
description: "Spark Steaming算子之mapwithstate和updatebykey"
first_time: "2017-05-02 14:52:32"
last_time: "2017-05-02 14:52:32"
---
> 背景：
> 在Spark Streaming中，数据是源源不断流进来的，有时我们需要对数据进行一 些周期性的统计，此时就需要维护一下数据的状态。     <br />
> 相关算子：
> (1).updateStateByKey
> (2).mapWithState

### 1.updateStateByKey  <br />
先获取上一个batch中的状态RDD和当前batch的RDD 做cogroup 得到一个新的状态RDD。这种方式完美的契合了RDD的不变性，但是对性能却会有比较大的影响,因为需要对所有数据做处理，计算量和数据集大小是成线性相关的。

* 源码     <br />
看一下updateStateByKey的代码，在Dstream中并没有找到updateStateByKey()方法，因为updateStateByKey是针对Key-Value的操作，所在可以想到updateStateByKey()方法其实是在PairDStreamFunctions类中，他是通过隐式转换的方式实现的。  <br />
```
implicit def toPairDStreamFunctions[K, V](stream: DStream[(K, V)])    (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null):  PairDStreamFunctions[K, V] = {  
   new PairDStreamFunctions[K, V](stream)
}
```
接着看updateStateByKey()方法,他有几种重载方式，最终调用以下的updateStateByKey()方法，代码如下:   <br />
```
def updateStateByKey[S: ClassTag](
   updateFunc: (Iterator[(K, Seq[V], Option[S])]) => Iterator[(K, S)],
   partitioner: Partitioner,
   rememberPartitioner: Boolean
 ): DStream[(K, S)] = ssc.withScope {
  new StateDStream(self, ssc.sc.clean(updateFunc), partitioner, rememberPartitioner, None)
}
```
这里实例化了一个StateDStream，看一下StateDStream的compute方法，代码如下:     <br />
```
override def compute(validTime: Time): Option[RDD[(K, S)]] = {

 // Try to get the previous state RDD
 getOrCompute(validTime - slideDuration) match {

   case Some(prevStateRDD) => {    // If previous state RDD exists

     // Try to get the parent RDD
     parent.getOrCompute(validTime) match {
       case Some(parentRDD) => {   // If parent RDD exists, then compute as usual
         computeUsingPreviousRDD (parentRDD, prevStateRDD)
       }
       case None => {    // If parent RDD does not exist

         // Re-apply the update function to the old state RDD
         val updateFuncLocal = updateFunc
         val finalFunc = (iterator: Iterator[(K, S)]) => {
           val i = iterator.map(t => (t._1, Seq[V](), Option(t._2)))
           updateFuncLocal(i)
         }
         val stateRDD = prevStateRDD.mapPartitions(finalFunc, preservePartitioning)
         Some(stateRDD)
       }
     }
   }

   case None => {    // If previous session RDD does not exist (first input data)

     // Try to get the parent RDD
     parent.getOrCompute(validTime) match {
       case Some(parentRDD) => {   // If parent RDD exists, then compute as usual
         initialRDD match {
           case None => {
             // Define the function for the mapPartition operation on grouped RDD;
             // first map the grouped tuple to tuples of required type,
             // and then apply the update function
             val updateFuncLocal = updateFunc
             val finalFunc = (iterator : Iterator[(K, Iterable[V])]) => {
               updateFuncLocal (iterator.map (tuple => (tuple._1, tuple._2.toSeq, None)))
             }

             val groupedRDD = parentRDD.groupByKey (partitioner)
             val sessionRDD = groupedRDD.mapPartitions (finalFunc, preservePartitioning)
             // logDebug("Generating state RDD for time " + validTime + " (first)")
             Some (sessionRDD)
           }
           case Some (initialStateRDD) => {
             computeUsingPreviousRDD(parentRDD, initialStateRDD)
           }
         }
       }
       case None => { // If parent RDD does not exist, then nothing to do!
         // logDebug("Not generating state RDD (no previous state, no parent)")
         None
       }
     }
   }
 }
}
```
这里代码分几种情况，但最终都调用computeUsingPreviousRDD()方法，关键操作就在computeUsingPreviousRDD()方法中，代码如下:
```
private [this] def computeUsingPreviousRDD (
 parentRDD : RDD[(K, V)], prevStateRDD : RDD[(K, S)]) = {
 // Define the function for the mapPartition operation on cogrouped RDD;
 // first map the cogrouped tuple to tuples of required type,
 // and then apply the update function
 val updateFuncLocal = updateFunc
 val finalFunc = (iterator: Iterator[(K, (Iterable[V], Iterable[S]))]) => {
   val i = iterator.map(t => {
     val itr = t._2._2.iterator
     val headOption = if (itr.hasNext) Some(itr.next()) else None
     (t._1, t._2._1.toSeq, headOption)
   })
   updateFuncLocal(i)
 }
 val cogroupedRDD = parentRDD.cogroup(prevStateRDD, partitioner)
 val stateRDD = cogroupedRDD.mapPartitions(finalFunc, preservePartitioning)
 Some(stateRDD)
}

```
可以看到当前状态的RDD和前一个状态的RDD进行cogroup操作
```
val cogroupedRDD = parentRDD.cogroup(prevStateRDD, partitioner)
```
parentRDD中只要有一条数据就会进行cogroup操作的，并将所有数据都进行更新函数（用户定义的）的操作，所以当数据量不断增加的时候，计算量随着线性增加。

###2.mapWithState
Spark 1.6之后出现。
他是一种变通的实现，因为没法变更RDD/Partition等核心概念，所以Spark Streaming在集合元素上做了文章，定义了MapWithStateRDD，将该RDD的元素做了限定，必须是MapWithStateRDDRecord 这个东西。该MapWithStateRDDRecord 保存分区内的所有key的状态(通过stateMap记录)以及计算结果(mappedData),元素MapWithStateRDDRecord 是可变的，但是RDD 依然是不变的。

* 源码
mapWithStage和updateStateByKey一样都是在PairDtreamFuntions类中,mapWithStage代码如下：
```
@Experimental
def mapWithState[StateType: ClassTag, MappedType: ClassTag](
   spec: StateSpec[K, V, StateType, MappedType]
 ): MapWithStateDStream[K, V, StateType, MappedType] = {
 new MapWithStateDStreamImpl[K, V, StateType, MappedType](
   self, spec.asInstanceOf[StateSpecImpl[K, V, StateType, MappedType]]
 )
}
```
首先看注解，他是一个实验性的方法，官方还没有推荐使用。
再看spec: StateSpec[K, V, StateType, MappedType]，这里并没有接收一个函数，而是一个StateSpec。其实就将函数包装在StateSpec内部而已
这里实例化了一个MapWithStateDStreamImpl，代码如下
```
private[streaming] class MapWithStateDStreamImpl[
 KeyType: ClassTag, ValueType: ClassTag, StateType: ClassTag, MappedType: ClassTag](
 dataStream: DStream[(KeyType, ValueType)],
 spec: StateSpecImpl[KeyType, ValueType, StateType, MappedType])
extends MapWithStateDStream[KeyType, ValueType, StateType, MappedType](dataStream.context) {

private val internalStream = new InternalMapWithStateDStream[KeyType, ValueType, StateType, MappedType](dataStream, spec)

override def slideDuration: Duration = internalStream.slideDuration

override def dependencies: List[DStream[_]] = List(internalStream)

override def compute(validTime: Time): Option[RDD[MappedType]] = {
 internalStream.getOrCompute(validTime).map(x=>{
   x.flatMap[MappedType](_.mappedData )
 })
}

/**
* Forward the checkpoint interval to the internal DStream that computes the state maps. This
* to make sure that this DStream does not get checkpointed, only the internal stream.
*/
override def checkpoint(checkpointInterval: Duration): DStream[MappedType] = {
 internalStream.checkpoint(checkpointInterval)
 this
}

/** Return a pair DStream where each RDD is the snapshot of the state of all the keys. */
def stateSnapshots(): DStream[(KeyType, StateType)] = {
 internalStream.flatMap {
   _.stateMap.getAll().map { case (k, s, _) => (k, s) }.toTraversable }
}

def keyClass: Class[_] = implicitly[ClassTag[KeyType]].runtimeClass

def valueClass: Class[_] = implicitly[ClassTag[ValueType]].runtimeClass

def stateClass: Class[_] = implicitly[ClassTag[StateType]].runtimeClass

def mappedClass: Class[_] = implicitly[ClassTag[MappedType]].runtimeClass
}
```
MapWithStateDStreamImpl的compute操作其他没有什么内容，主要是从internalStream中获取计算结果，internalStream是在MapWithStateDStreamImpl实例化的时候创建，代码如下
```
private val internalStream = new InternalMapWithStateDStream[KeyType, ValueType, StateType, MappedType](dataStream, spec)
```
看 InternalMapWithStateDStream的compute方法，代码如下
```
override def compute(validTime: Time): Option[RDD[MapWithStateRDDRecord[K, S, E]]] = {
 // Get the previous state or create a new empty state RDD
 // 得到以前状态的RDD或创建一个空状态的RDD
 val prevStateRDD = getOrCompute(validTime - slideDuration) match {
   case Some(rdd) =>
     if (rdd.partitioner != Some(partitioner)) {
       // If the RDD is not partitioned the right way, let us repartition it using the
       // partition index as the key. This is to ensure that state RDD is always partitioned
       // before creating another state RDD using it
       MapWithStateRDD.createFromRDD[K, V, S, E](rdd.flatMap(_.stateMap.getAll()), partitioner, validTime)
     } else {
       rdd
     }
   case None =>
     MapWithStateRDD.createFromPairRDD[K, V, S, E](
       // 获取用户初始化的状态RDD
       spec.getInitialStateRDD().getOrElse(new EmptyRDD[(K, S)](ssc.sparkContext)),
       partitioner,
       validTime
     )
 }
 // Compute the new state RDD with previous state RDD and partitioned data RDD
 // Even if there is no data RDD, use an empty one to create a new state RDD
 // 获取当前要进行计算的RDD
 val dataRDD = parent.getOrCompute(validTime).getOrElse {
   context.sparkContext.emptyRDD[(K, V)]
 }
 val partitionedDataRDD = dataRDD.partitionBy(partitioner)

 val timeoutThresholdTime = spec.getTimeoutInterval().map { interval =>
   (validTime - interval).milliseconds
 }
 Some(new MapWithStateRDD(prevStateRDD, partitionedDataRDD, mappingFunction, validTime, timeoutThresholdTime))
}
```
首先获取前一个状态的RDD（prevStateRDD），prevStateRDD在第一次使用的时候调用MapWithStateRDD.createFromPairRDD方法，将自用定义的初始值放在到新生成的stageMap中;如果prevStateRDD分区和当前状态分区不同时会调用MapWithStateRDD.createFromRDD()将状态数据重新分区后放入新生成的stageMap，createFromRDD()方法和createFromPairRDD方法代码如下
```
def createFromPairRDD[K: ClassTag, V: ClassTag, S: ClassTag, E: ClassTag](
   pairRDD: RDD[(K, S)],
   partitioner: Partitioner,
   updateTime: Time): MapWithStateRDD[K, V, S, E] = {

 val stateRDD = pairRDD.partitionBy(partitioner).mapPartitions ({ iterator =>
   val stateMap = StateMap.create[K, S](SparkEnv.get.conf)
   // 把用户定义的初始值放入新创建的stateMap
   iterator.foreach { case (key, state) => stateMap.put(key, state, updateTime.milliseconds) }
   // 把stateMap放在MapWithStateRDDRecord中做为RDD的元素返回
   Iterator(MapWithStateRDDRecord(stateMap, Seq.empty[E]))
 }, preservesPartitioning = true)

 val emptyDataRDD = pairRDD.sparkContext.emptyRDD[(K, V)].partitionBy(partitioner)

 val noOpFunc = (time: Time, key: K, value: Option[V], state: State[S]) => None

 new MapWithStateRDD[K, V, S, E](stateRDD, emptyDataRDD, noOpFunc, updateTime, None)
}
def createFromRDD[K: ClassTag, V: ClassTag, S: ClassTag, E: ClassTag](
   rdd: RDD[(K, S, Long)],
   partitioner: Partitioner,
   updateTime: Time): MapWithStateRDD[K, V, S, E] = {

 val pairRDD = rdd.map { x => (x._1, (x._2, x._3)) }
 val stateRDD = pairRDD.partitionBy(partitioner).mapPartitions({ iterator =>
   val stateMap = StateMap.create[K, S](SparkEnv.get.conf)
   // 把之前stateMap中的状态数据（key,(state,update)）放入一个stateMap中
   iterator.foreach { case (key, (state, updateTime)) =>
     stateMap.put(key, state, updateTime)
   }
   Iterator(MapWithStateRDDRecord(stateMap, Seq.empty[E]))
 }, preservesPartitioning = true)

 val emptyDataRDD = pairRDD.sparkContext.emptyRDD[(K, V)].partitionBy(partitioner)

 val noOpFunc = (time: Time, key: K, value: Option[V], state: State[S]) => None

 new MapWithStateRDD[K, V, S, E](stateRDD, emptyDataRDD, noOpFunc, updateTime, None)
}
```
prevStateRDD获取之后实例化MapWithStateRDD，将前一个状态RDD和当前要计算的RDD传递进去，看MapWithStateRDD类的代码
```
private[streaming] class MapWithStateRDD[K: ClassTag, V: ClassTag, S: ClassTag, E: ClassTag](
 // 存储State数据的RDD
 private var prevStateRDD: RDD[MapWithStateRDDRecord[K, S, E]],
 // 计算当前数据的RDD
 private var partitionedDataRDD: RDD[(K, V)],
 // 计算函数
 mappingFunction: (Time, K, Option[V], State[S]) => Option[E],
 batchTime: Time,
 timeoutThresholdTime: Option[Long]
) extends RDD[MapWithStateRDDRecord[K, S, E]](
 partitionedDataRDD.sparkContext,
 // MapWithStateRDD依赖两个父RDD，因为有两个数据来源。一个是状态数据，一个是当前数据
 List(
   new OneToOneDependency[MapWithStateRDDRecord[K, S, E]](prevStateRDD),
   new OneToOneDependency(partitionedDataRDD))
) {

@volatile private var doFullScan = false

require(prevStateRDD.partitioner.nonEmpty)
require(partitionedDataRDD.partitioner == prevStateRDD.partitioner)

override val partitioner = prevStateRDD.partitioner

override def checkpoint(): Unit = {
 super.checkpoint()
 doFullScan = true
}

override def compute(partition: Partition, context: TaskContext): Iterator[MapWithStateRDDRecord[K, S, E]] = {

 val stateRDDPartition = partition.asInstanceOf[MapWithStateRDDPartition]
 val prevStateRDDIterator = prevStateRDD.iterator(stateRDDPartition.previousSessionRDDPartition, context)
 val dataIterator = partitionedDataRDD.iterator(stateRDDPartition.partitionedDataRDDPartition, context)

 // 因为prevStateRDD只有一个元素，所有取prevStateRDDIterator.next()
 val prevRecord:Option[MapWithStateRDDRecord[K, S, E]] = if (prevStateRDDIterator.hasNext){
   Some(prevStateRDDIterator.next())
 }
 else {
   None
 }

 // 返回一个新的MapWithStateRDDRecord
 val newRecord = MapWithStateRDDRecord.updateRecordWithData(
   prevRecord,
   dataIterator,
   mappingFunction,
   batchTime,
   timeoutThresholdTime,
   removeTimedoutData = doFullScan // remove timedout data only when full scan is enabled
 )
 // 将新生成的MapWithStateRDDRecord放入迭代器，此迭代器还是只有一个元素
 Iterator(newRecord)
}

override protected def getPartitions: Array[Partition] = {
 Array.tabulate(prevStateRDD.partitions.length) { i =>
   new MapWithStateRDDPartition(i, prevStateRDD, partitionedDataRDD)}
}

override def clearDependencies(): Unit = {
 super.clearDependencies()
 prevStateRDD = null
 partitionedDataRDD = null
}

def setFullScan(): Unit = {
 doFullScan = true
}
}
```
主要看newRecord是怎样生成的，因为newRecord里有所有的状态信息和计算结果，看 MapWithStateRDDRecord.updateRecordWithData的代码
```
def updateRecordWithData[K: ClassTag, V: ClassTag, S: ClassTag, E: ClassTag](
 // 前一个MapWithStateRDDRecord
 prevRecord: Option[MapWithStateRDDRecord[K, S, E]],
 // 当前需要计算的数据
 dataIterator: Iterator[(K, V)],
 // 计算函数
 mappingFunction: (Time, K, Option[V], State[S]) => Option[E],
 batchTime: Time,
 timeoutThresholdTime: Option[Long],
 removeTimedoutData: Boolean
): MapWithStateRDDRecord[K, S, E] = {
 // Create a new state map by cloning the previous one (if it exists) or by creating an empty one

 // 首先创建一个新的StateMap,这里是从前一个StageMap复制而来的，由于StageMap的复制是采用增量复制，
 // 新创建的stateMap会引用旧的stateMap
 val newStateMap = prevRecord.map( _.stateMap.copy()). getOrElse { new EmptyStateMap[K, S]() }

 val mappedData = new ArrayBuffer[E]
 val wrappedState = new StateImpl[S]()

 // Call the mapping function on each record in the data iterator, and accordingly
 // update the states touched, and collect the data returned by the mapping function
 // mapWithStage操作性能优势就是在这里体现的
 dataIterator.foreach { case (key, value) =>
   //将newStateMap中的元素包装一下
   wrappedState.wrap(newStateMap.get(key))
   // 终于看到用户定义的mappingFunction函数了，传入当前key，当前value,和此key的历史数据
   val returned = mappingFunction(batchTime, key, Some(value), wrappedState)
   if (wrappedState.isRemoved) {
     // 如果更新值被标记删除
     newStateMap.remove(key)
   } else if (wrappedState.isUpdated || (wrappedState.exists && timeoutThresholdTime.isDefined)) {
     // 如果当前key的value为标记有更新，就更新newStateMap，重新put操作
     newStateMap.put(key, wrappedState.get(), batchTime.milliseconds)
   }
   mappedData ++= returned
 }

 // Get the timed out state records, call the mapping function on each and collect the
 // data returned
 if (removeTimedoutData && timeoutThresholdTime.isDefined) {
   newStateMap.getByTime(timeoutThresholdTime.get).foreach { case (key, state, _) =>
     wrappedState.wrapTimingOutState(state)
     val returned = mappingFunction(batchTime, key, None, wrappedState)
     mappedData ++= returned
     newStateMap.remove(key)
   }
 }
 // newStateMap 状态集合
 // mappedData 返回计算后的结果，这里要注意：因为上面的迭代操作是基于当前RDD的数据，
 // 所以返回计算后的结果只有当前数据的更新值
 MapWithStateRDDRecord(newStateMap, mappedData)
}
```
通过上面的注释已经知道MapWithStateRDD[MapWithStateRDDRecord]类型的RDD的数据是怎么计算的了，接着看InternalMapWithStateDStream的computer方法返回后的操作
```
override def compute(validTime: Time): Option[RDD[MappedType]] = {
  internalStream.getOrCompute(validTime).map(x=>{
      x.flatMap[MappedType](_.mappedData )
  })
}
```
使用flatMap从MapWithStateRDDRecord中获取mappedData(当前RDD进行状态计算后的结果)并返回，到这里mapWithStage的操作就完成了
###3.Demo
wordCountDemo代码：
```
import _root_.kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming._
object StateWordCount {
def main(args: Array[String]): Unit = {
 val topics = "kafkaforspark"
 val brokers = "*.*.*.*:9092,*.*.*.*:9092"
 val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount").setMaster("local[2]")
 sparkConf.set("spark.testing.memory", "2147480000")
 val ssc = new StreamingContext(sparkConf, Seconds(5))
 ssc.checkpoint("hdfs://mycluster/ceshi/")
 val topicsSet = topics.split(",").toSet
 val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
 val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
 .map(_._2.trim).map((_, 1))
 // 1. mapWithStage操作
 val mappingFunc = (word: String, one: Option[Int], state: State[Int]) => {
   val sum = one.getOrElse(0) + state.getOption.getOrElse(0)
   val output = (word, sum)
   state.update(sum)
   output
 }
 val state = StateSpec.function(mappingFunc)
 messages.reduceByKey(_+_).mapWithState(state).print()
// 2. updateStateByKey
//    val addFunc = (currValues: Seq[Int], prevValueState: Option[Int]) => {
//      //通过Spark内部的reduceByKey按key规约，然后这里传入某key当前批次的Seq/List,再计算当前批次的总和
//      val currentCount = currValues.sum
//      // 已累加的值
//      val previousCount = prevValueState.getOrElse(0)
//      // 返回累加后的结果，是一个Option[Int]类型
//      Some(currentCount + previousCount)
//    }
//    messages.updateStateByKey(addFunc,2).print()
 ssc.start()
 ssc.awaitTermination()
}
}
```
输入数据：
```
 1
 2
 3
 4
 5

 4
 5
 6
 7
 8

 6
 7
 8
 9
```

updateStateByKey结果：
```
 -------------------------------------------
 Time: 1464516940000 ms
 -------------------------------------------
 (4,1)
 (2,1)
 (5,1)
 (3,1)
 (1,1)
 -------------------------------------------
 Time: 1464516945000 ms
 -------------------------------------------
 (4,2)
 (8,1)
 (6,1)
 (2,1)
 (7,1)
 (5,2)
 (3,1)
 (1,1)
 -------------------------------------------
 Time: 1464516950000 ms
 -------------------------------------------
 (8,2)
 (4,2)
 (6,2)
 (2,1)
 (7,2)
 (5,2)
 (9,1)
 (3,1)
 (1,1)
```
mapWithState结果：
```
 (4,1)
 (5,1)
 (1,1)
 (2,1)
 (3,1)
 -------------------------------------------
 Time: 1464516190000 ms
 -------------------------------------------
 (4,2)
 (8,1)
 (5,2)
 (6,1)
 (7,1)
 -------------------------------------------
 Time: 1464516195000 ms
 -------------------------------------------
 (8,2)
 (9,1)
 (6,2)
 (7,2)
```
####Demo结论：
看以上两种操作返回的结果是不一样的，mapWithStage返回最新数据的状态结果，而updateStateByKey返回了所有状态结果，具体使用要配合业务进行调整
