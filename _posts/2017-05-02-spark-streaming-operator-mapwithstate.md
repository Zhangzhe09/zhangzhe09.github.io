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

###1.updateStateByKey
先获取上一个batch中的状态RDD和当前batch的RDD 做cogroup 得到一个新的状态RDD。这种方式完美的契合了RDD的不变性，但是对性能却会有比较大的影响,因为需要对所有数据做处理，计算量和数据集大小是成线性相关的。

* 源码
看一下updateStateByKey的代码，在Dstream中并没有找到updateStateByKey()方法，因为updateStateByKey是针对Key-Value的操作，所在可以想到updateStateByKey()方法其实是在PairDStreamFunctions类中，他是通过隐式转换的方式实现的。
###2.mapWithState

* 源码

