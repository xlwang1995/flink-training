<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Lab: Stateful Enrichment (Rides and Fares)
# 实验：本练习的目标是将每次乘车的“TaxiRide”和“TaxiFare”记录连接在一起。

对于每个不同的 `rideId`，恰好有三个事件：

1. 一个`TaxiRide` START事件
1. 一个`TaxiRide` END事件
1. 一个 `TaxiFare` 事件（其时间戳恰好与开始时间匹配） 

该实验的结果应该是一个'DataStream<Tuple2<TaxiRide, TaxiFare>>'，
每个不同的rideId 都有一个记录。 每个元组应该将某个rideId 的TaxiRide START 事件与其匹配的TaxiFare 配对。

### 输入数据

在本练习中，您将使用两个数据流，一个是由“TaxiRideSource”生成的“TaxiRide”事件，另一个是由“TaxiFareSource”生成的“TaxiFare”事件。 
有关如何使用这些流生成器的信息，请参阅 [使用出租车数据流](../README.md#using-the-taxi-data-streams)。

For this exercise you will work with two data streams, one with `TaxiRide` events generated by a `TaxiRideSource` and the other with `TaxiFare` events generated by a `TaxiFareSource`. See [Using the Taxi Data Streams](../README.md#using-the-taxi-data-streams) for information on how to work with these stream generators.

### 期望输出

本练习的结果是一个包含 `Tuple2<TaxiRide, TaxiFare>` 记录的数据流，每个不同的 `rideId` 对应一个。 
该练习设置为忽略 END 事件，您应该在每次骑行的 START 与其对应的票价事件中加入该事件。

结果流被打印到标准输出。

## 入门

> :information_source: Rather than following these links to the sources, you might prefer to open these classes in your IDE.

### Exercise Classes

- Java:  [`org.apache.flink.training.exercises.ridesandfares.RidesAndFaresExercise`](src/main/java/org/apache/flink/training/exercises/ridesandfares/RidesAndFaresExercise.java)
- Scala: [`org.apache.flink.training.exercises.ridesandfares.scala.RidesAndFaresExercise`](src/main/scala/org/apache/flink/training/exercises/ridesandfares/scala/RidesAndFaresExercise.scala)

### 测试

- Java:  [`org.apache.flink.training.exercises.ridesandfares.RidesAndFaresTest`](src/test/java/org/apache/flink/training/exercises/ridesandfares/RidesAndFaresTest.java)
- Scala: [`org.apache.flink.training.exercises.ridesandfares.scala.RidesAndFaresTest`](src/test/scala/org/apache/flink/training/exercises/ridesandfares/scala/RidesAndFaresTest.scala)

## 实现提示

<details>
<summary><strong>程序结构</strong></summary>


您可以使用“RichCoFlatMap”来实现此连接操作。 请注意，您无法控制每个rideId 的乘车到达顺序和票价记录，因此您需要准备好存储任何一条信息，直到匹配信息到达，此时您可以发出一个 `Tuple2 <TaxiRide, TaxiFare>` 将两个记录连接在一起。

You can use a `RichCoFlatMap` to implement this join operation. Note that you have no control over the order of arrival of the ride and fare records for each rideId, so you'll need to be prepared to store either piece of information until the matching info arrives, at which point you can emit a `Tuple2<TaxiRide, TaxiFare>` joining the two records together.
</details>

<details>
<summary><strong>Working with State</strong></summary>

您应该使用 Flink 的托管键控状态来缓冲保存的数据，直到匹配事件到达。 一旦不再需要它，一定要清除状态。

You should be using Flink's managed, keyed state to buffer the data that is being held until the matching event arrives. And be sure to clear the state once it is no longer needed.
</details>

## Discussion

出于本练习的目的，可以假设 START 和票价事件完美配对。 但是在现实世界的应用程序中，你应该担心这样一个事实，即每当一个事件丢失时，同一个 `rideId` 的另一个事件将永远保持在 state 中。 在 [稍后的实验室](../long-ride-alerts) 中，
我们将查看 `ProcessFunction` 和 Timers，它们也可能有助于解决这里的情况。
## Documentation

- [Working with State](https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/state/index.html)

## Reference Solutions

Reference solutions are available in this project:

- Java:  [`org.apache.flink.training.solutions.ridesandfares.RidesAndFaresSolution`](src/solution/java/org/apache/flink/training/solutions/ridesandfares/RidesAndFaresSolution.java)
- Scala: [`org.apache.flink.training.solutions.ridesandfares.scala.RidesAndFaresSolution`](src/solution/scala/org/apache/flink/training/solutions/ridesandfares/scala/RidesAndFaresSolution.scala)

-----

[**Back to Labs Overview**](../LABS-OVERVIEW.md)
