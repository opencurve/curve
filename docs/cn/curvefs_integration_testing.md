# CurveFS 集成测试方案

## 目的

集成测试是在单元测试的基础上，将所有的软件单元按照概要设计规格说明的要求组装成模块、子系统进行测试。发生在单元测试之后和系统测试之前，用于验证不同模块间的接口调用、交互逻辑是否符合预期。

### 与单元测试区别

单元测试的关注点是一个范围很小的单元，通常是一个函数或一处关键逻辑。对于完成这个函数功能所依赖的外部组件，比如文件系统、数据库、网络请求等，进行 mock 或用 fake 对象代替。而集成测试通过组合相互依赖的单元/模块得到子系统，针对子系统进行测试。

### 与集成测试区别

系统测试属于黑盒测试，重点关注系统整体功能是否正常，异常场景下系统的表现是否符合预期等。而集成测试属于白盒或灰盒测试，可以更细粒度的关注处理流程。同时，在不同的测试方法中，比如自底向上集成，可以只针对某一个具体的功能进行测试，而不需要搭建整个集群。

## 测试方法

集成测试主要有两种执行方式，一种是一次性将所有单元组装起来进行测试的“大爆炸”模式。另外一种是层层递进的模式，每次集成一个新的模块进行测试，直至所有模块都组装完成，这里的递进也分为两种：自底向上和自顶向下。

### 大爆炸集成方法

在完成所有模块开发及单元测试后，将所有单元/模块组装到一起进行测试。这种方法与系统测试是有区别的，这里集成之后的测试重点还是各个子系统的功能，以及不同子系统之间的交互。而系统测试是整体功能的测试。

![big-bang-testing](https://www.softwaretestinghelp.com/wp-content/qa/uploads/2016/12/integration-testing_big-bang-approach.png)

### 自顶向下集成方法

从上到下逐步测试单元/模块的集成。首先测试较高级别的模块，然后再测试和集成较低级别的模块，以检查软件功能。对于测试中未完成的底层模块，通过编写测试桩（stub）替代。

![top-down-testing](https://www.softwaretestinghelp.com/wp-content/qa/uploads/2016/12/integration-testing_top-down-approach.jpeg)

### 自底向上集成方法

单元/模块从底层到顶层，一步一步地进行测试，直到所有级别的单元/模块都集成在一起并作为一个整体进行测试。同样，在部分上层模块未完成开发是，需要编写测试驱动（driver）来启动测试。

![bottom-up-testing](https://www.softwaretestinghelp.com/wp-content/qa/uploads/2016/12/integration-testing_bottom-up.jpeg)

## 测试内容

### 功能性测试

根据详细设计中模块提供的功能进行完备的测试。在做功能测试前需要设计充分的测试用例，考虑各种系统状态和参数输入下模块依然能够正常工作，并返回预期的结果。

在这一过程中，需要有一种科学的用例设计方法，依据这种方法可以充分考虑各种输入场景，保证测试功能不被遗漏，同时能够以尽可能少的用例和执行步骤完成测试过程。

### 异常测试

异常测试是有别于功能测试和性能测试又一种测试类型，通过异常测试，可以发现由于系统异常、依赖服务异常、应用本身异常等原因引起的性能瓶颈，提高系统的稳定性。

常见的异常如：磁盘错误、网络错误、数据出错、程序重启等等。

### 压力测试

功能性的测试更多是单线程的测试，还需要在并发场景下对模块进行测试，观察在并发场景下是否能够正常工作，逻辑或者数据是否会出现错误。

考虑并发度时，使用较大的压力来测试，例如平时使用的2倍、10倍或更大的压力。

## CurveBS 集成测试方案

前面说到，集成测试通常是在单元测试之后，系统测试之前完成，用于发现单元/模块组合过程中的问题。当前 FS 已经发布了两个 beta 版本，都经过 QA 完整的测试。结合之前 CurveBS 集成测试的经验，完成所有模块的集成测试，包括用例设计、代码编写、review 等，可能需要花费数周的时间。所以，整体来看，补充现有模块的集成测试收益不是很明显。但是，后续新特性的开发、bug 修复等，可以添加相应的集成测试。

### 新增功能集成测试

在当前的开发流程中，开发人员完成方案设计、代码开发、单元测试及代码 review、通过持续集成测试后，就可以将代码合入仓库。

在版本提测前，开发人员编写对应功能的测试用例，然后进行集群部署、人工测试、bug 修复，待所有功能完成自测后，提交版本给 QA 进行系统测试。单个功能的测试工作量在两个工作日左右。**这里的功能自测，其实就是对应功能的集成测试**。如果能在开发过程中，以集成测试代码的形式完成上述过程，可能会加快版本发布的流程。

### Bug 修复集成测试

以 [kudu](https://github.com/apache/kudu) 为例，[集成测试](https://github.com/apache/kudu/tree/master/src/kudu/integration-tests)中除了基本功能和压力测试外，包含了大量的针对 bug 的回归测试（100多个）。例如

```cpp
// Regression test for KUDU-1551: if the tserver crashes after preallocating a segment
// but before writing its header, the TS would previously crash on restart.
// Instead, it should ignore the uninitialized segment.
TEST_P(TsRecoveryITest, TestCrashBeforeWriteLogSegmentHeader) {
  NO_FATALS(StartClusterOneTs({
    "--log_segment_size_mb=1",
    "--log_compression_codec=NO_COMPRESSION"
  }));
  TestWorkload work(cluster_.get());
  work.set_num_replicas(1);
  work.set_write_timeout_millis(1000);
  work.set_timeout_allowed(true);
  work.set_payload_bytes(10000); // make logs roll without needing lots of ops.
  work.Setup();

  // Enable the fault point after creating the table, but before writing any data.
  // Otherwise, we'd crash during creation of the tablet.
  ASSERT_OK(cluster_->SetFlag(cluster_->tablet_server(0),
                              "fault_crash_before_write_log_segment_header", "0.9"));
  work.Start();

  // Wait for the process to crash during log roll.
  ASSERT_OK(cluster_->tablet_server(0)->WaitForInjectedCrash(MonoDelta::FromSeconds(60)));
  work.StopAndJoin();

  cluster_->tablet_server(0)->Shutdown();
  ignore_result(cluster_->tablet_server(0)->Restart());

  ClusterVerifier v(cluster_.get());
  NO_FATALS(v.CheckRowCount(work.table_name(),
                            ClusterVerifier::AT_LEAST,
                            work.rows_inserted()));
}
```

用例描述了该测试用相应的 [issue](https://issues.apache.org/jira/browse/KUDU-1551)、触发场景等，集成测试用例与修复代码一起提交合入，以保证功能修复的正确性，也可以确保后续的代码修改不会对这次的修复产生影响。

同时，当前 FS 也加入了[自动化测试](https://github.com/opencurve/curve/blob/fs/robot/curve_fs_robot.txt)，覆盖了基本功能、常见异常、数据一致性等测试。所以，在添加集成测试用例时，也需要考虑自动化测试是否可以覆盖需要测试的场景，如果可以，就不需要添加相应的集成测试用例。反之，如果没有覆盖，或者覆盖不完全，则需要添加。

所以，综合以上的讨论，是否添加相应的集成测试，可以参考如下的判断：

- 是否需要多模块协作
  - 如果是，则需要再完成单元测试后，添加整体功能的集成测试。
- 已有的自动化测试是否能够覆盖主要流程
  - 如果不能，则可以根据工作量、改动情况再决定添加自动化测试或集成测试。
- 是否影响现有的功能
  - 如果是，则同时需要对现有功能添加相应的集成测试，保证现有功能的正确性。
- ...

## 集成测试用例设计方法

测试用例是为验证程序是否符合特定系统需求而开发的测试输入、执行条件和预期结果的集合，其组织性、功能覆盖性、重复性的特点能够保证测试功能不被遗漏。由于测试用例往往涉及多重选择、循环嵌套，不同的路径数目可能非常大，所以必须精心设计使之能达到最佳的测试效果。

### 设计原则

这里首先需要考虑从什么样的角度去思考用例的设计，一种是以接口的角度，根据接口划分来设计用例；另一种是以使用场景的角度来设计。

#### 根据接口来设计用例

优点：可以根据输入不同的接口参数，产生不同的预期来设计用例，考虑比较完整地考虑各种调用情况。

缺点：根据接口提供的功能来设计用例，有时候难以考虑一些特殊场景下功能上的缺陷。

#### 根据场景来设计用例

优点：用例审核者能比较容易理解各组用例出现的场景；可以测试出功能设计之初未考虑到的一些场景。

缺点：无法很好地证明用例覆盖是否充分。

两种方式各有优缺点，可以考虑将两种方式结合起来，用接口方式来设计用例，然后用场景方式来组织用例的执行序列。

根据接口进行设计可以比较清晰地评估用例覆盖是否完全，然后以场景方式来执行这些用例，对执行过的用例就打钩记录；这样可以很清楚的知道哪些用例执行了哪些没执行，帮助发现没有想到的场景；

此外还能通过特殊的场景帮助发现接口功能是否考虑充分，两种方式可以互补帮助发现更多问题。

### 用例模板

用例设计可以遵循GWT（Given-When-Then）的模式来写，Given表示给定的前提条件，When表示要发生的操作，Then表示预期的结果。

如果要覆盖所有的用例，那么势必要列出所有的前提条件，然后在特定的前提下，需要列出所有可能发生的操作。

| 编号 | Given | When | Than | 备注 | 是否执行 |
| ---- | ---- | ---- | ----- | --- | ------ |
| 1 | x | x | x | x | x |

用例设计可以参考之前 CurveBS Datastore 的集成测试用例(https://github.com/opencurve/curve/blob/master/docs/cn/quality-integration-example.md)。

## 模糊测试

无论是单元测试、集成测试还是自动化测试，都需要开发或测试人员设计测试用例并编写相应的代码进行测试，同时，这些测试通常只对主要流程进行针对性的测试。而模糊测试（Fuzzing、Fuzz Testing）是一种自动化的测试方法，可以向系统提供非法、超出预期或随机的数据，并监控系统行为是否异常，以发现可能的错误。

[OSS-Fuzz](https://github.com/google/oss-fuzz) 是当前比较知名的模糊测试框架之一，由 Google 开源，支持多种开发语言，截止到 2022 年 1 月份，已发现 550 个开源软件中的 36,000 多个 bug。

TODO:

- [ ] 如何使用？
- [ ] 是否适用？

## 参考

1 [The Differences Between Unit Testing, Integration Testing And Functional Testing](https://www.softwaretestinghelp.com/the-difference-between-unit-integration-and-functional-testing/)

2 [软件测试入门系列之十：集成测试](https://zhuanlan.zhihu.com/p/354967307)

3 [Fuzzing](https://en.wikipedia.org/wiki/Fuzzing)

4 [Announcing OSS-Fuzz: Continuous fuzzing for open source software](https://opensource.googleblog.com/2016/12/announcing-oss-fuzz-continuous-fuzzing.html)