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
> 在Spark Streaming中，数据是源源不断流进来的，有时我们需要对数据进行一 些周期性的统计，此时就需要维护一下数据的状态。
> 相关算子：
> (1).updateStateByKey
> (2).mapWithState
