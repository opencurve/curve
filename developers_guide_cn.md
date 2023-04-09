[English version](developers_guide.md)


## 概述

本文旨在帮助社区开发者了解 Curve 项目的全貌，更好的参与 Curve 项目的开发与演进。本文会站在社区参与者的角度进行描述如何更好的参与到 Curve 项目发展中来。

## 如何了解 Curve

参与开源项目的前提是了解它，特别是针对 Curve 这样的大型的复杂项目，入门难度较大，这里提供一些资料帮助那些对 Curve 项目感兴趣的同学：

- [Curve Overview](https://github.com/opencurve/curve/tree/master/docs)
- [Curve Architecture](http://www.opencurve.io/)
- [Curve Deploy](https://github.com/opencurve/curveadm/wiki)
- [Curve Design docs](https://github.com/opencurve/curve/tree/master/docs/cn)
- [Curve Code  Analysis](https://github.com/opencurve/curve/wiki/Curve源码及核心流程深度解读)
- [Curve FAQ](https://github.com/openCURVE/CURVE/wiki/CURVE-FAQ)

通过上面的资料的学习相信你已经对 Curve 项目有了一个整体的了解，同时可能也存在着一些好奇和疑问，这时可以部署一个 Curve 的体验环境，这样有利于更加直观的感知 Curve 系统，对于遇到了问题或者是想要新的 feature 可以追踪相关的代码，在这个过程中很容易对相关模块有了解，不少贡献者就是这样完成了第一次的贡献。

Curve 社区有多个[沟通渠道](#社区交流)，每两周还会有线上的社区例会，例会上会同步近期 Curve 项目的进展，解答大家的疑问，也欢迎大家参加 Curve 社区例会，在会上进行交流，这样能够快速解答大家的疑问，提升对 Curve 项目的了解和同步现阶段做的事情。

## 如何参与 Curve

在对 Curve 项目有一定了解后，如果感兴趣就可以选择入手点进行 Curve 开源项目的参与了，可以从以下几个方面进行选择：

- 从 Curve 项目的 [issue](https://github.com/opencurve/curve/issues) 中选择感兴趣的问题入手，可以特别关注带有 [good first issue](https://github.com/opencurve/curve/issues?q=is%3Aissue+is%3Aopen+sort%3Aupdated-desc+label%3A%22good+first+issue%22) tag 的 issue，这些是我们经过评估认为是相对好入门的一些问题。
- 在对 Curve 项目有一定了解的基础上也可以从 [Roadmap](https://github.com/opencurve/curve/wiki/Roadmap_CN) 中进行选择。
- 从 Curve 项目的运维工具 [Curveadm](https://github.com/opencurve/curveadm) 的 [issue](https://github.com/opencurve/curveadm/issues) 和 [Roadmap](https://github.com/opencurve/curveadm/issues/92) 中进行选择，开发方式可参见 [快速上手 CurveAdm 开发](https://github.com/opencurve/curveadm/wiki/develop)。
- 除了已有的 issue，也欢迎将自己发现的问题或希望的新特性提出 issue 并进行解决。
- 可以关注现有 Curve 代码中的 *TODO* ，大部分为代码逻辑优化和待补充特性，选择感兴趣的提出相关 issue 跟进并尝试解决。


## 如何进行方案提交

找到一个感兴趣的点之后，可以通过 issue 进行讨论，如果是一个小的 bug-fix 或者是功能点，可以在进行简单讨论之后开始着手开发。即使简单的问题，也建议先与我们进行沟通，以免出现对问题的理解出了偏差或解决方案有问题，做了无用功。

如果要做的事情比较复杂，需要先写一个详细的设计文档，提交到 curve/docs 下，这个目录下已有的设计方案供你参考。一篇好的设计方案要写清楚以下几点：

- 背景描述
- 解决什么问题
- 方案设计（方案描述，正确性和可行性），最好还能包括几种方案的对比
- 与现有系统的兼容性
- 方案的具体实现

在开始方案撰写之前同样建议先与我们进行沟通，对方案的可行性不太确定的可以先提供简单的方案描述，经过我们评估后再进行细化和开发。

## 如何进行文档贡献

Curve 的文档位于 [curve/docs/](https://github.com/opencurve/curve/tree/master/docs), 贡献请使用 markdown 格式(除了ppt).

文档贡献无需触发 CI, 请在 github pull request 标题头部加上 ```[skipci]```.

## 如何提交PR

在完成代码编写后，就可以提交 PR。当然如果开发尚未完成，在某些情况下也可以先提交 PR，比如希望先让社区看一下大致的解决方案，可以在完成代码框架后提价 PR。

[Curve 编译环境快速构建](https://github.com/opencurve/curve/blob/master/docs/cn/build_and_run.md)

[Curve 测试用例编译及执行](https://github.com/opencurve/curve/blob/master/docs/cn/build_and_run.md#测试用例编译及执行)

代码补全: 请使用 clangd 作为代码补全, 请见 [curve_clangd.md](https://github.com/opencurve/curve/blob/master/docs/cn/clangd.md).

Curve CI 使用```cpplint```检查更改的代码,
- 安装```cpplint``` (需要root):
  ```bash
  $ pip install cpplint
  ```
- 本地进行代码检查:
  ```bash
  $ cpplint --filter=-build/c++11 --quiet --recursive your_path
  ```

对于 PR 我们有如下要求：

- Curve编码规范严格按照[Google C++开源项目编码指南](https://zh-google-styleguide.readthedocs.io/en/latest/google-cpp-styleguide/contents/)来进行代码编写，但使用 4 空格进行缩进, 可使用 clang-format 进行格式化, CI 会检查相关更改代码是否符合规则.
- 代码必须有测试，文档除外，单元测试（增量行覆盖80%以上，增量分支覆盖70%以上）；集成测试（与单元测试合并统计，满足单元测试覆盖率要求即可）
- 请尽可能详细的填写 PR 的描述，关联相关问题的 issue，PR commit message 能清晰看出解决的问题，提交到 Curve master 分支之后会自动触发Curve CI，需保证 CI 通过，CI 的 Jenkins 用户名密码为 netease/netease，如遇到 CI 运行失败可以登录 Jenkins 平台查看失败原因。
- CI 通过之后可开始进行 review，每个 PR 在合并之前都需要至少得到两个 Committer/Maintainer 的 LGTM。
- PR 代码需要一定量的注释来使代码容易理解，且所有注释和 review 意见和回复均要求使用英语。

对于 commit message：

一条好的 commit message 需要包含以下要素：

1. What is your change?（必须）
2. Why this change was made?（必须）
3. What effect does the commit have? (可选)

说明提交的 PR 做了那些修改：性能优化？修复bug？增加功能？以及这么做的原因。最后描述以下这样修改带来的影响，包括性能等等。当然对一些简单的修改，修改的原因和影响可以忽略。在 message中尽量遵循以下原则：

- 总结说明 PR 的功能和作用
- 使用短句和简单的动词
- 避免长的复合词和缩写

在提交时请尽可能的遵循以下格式：

```
[type]<scope>: <description>
<BLANK LINE>
[body]
<BLANK LINE>
[footer]
```

type 可以是以下类型之一：

- build: 影响系统构建以及外部依赖
- ci: 影响持续继承相关的功能
- docs: 文档相关的修改
- feat: 增加新的特性
- fix: bug 修复
- perf: 性能提升
- refactor: 重构相关的代码，不增加功能也不修复错误
- style: 不影响代码的的含义的修改，仅仅是修改代码风格
- test: 单元测试相关的修改

第一行表示标题应尽可能保持 70 个字符以内，阐述修改的模块以及内容，多模块可以使用 `*` 来表示，并在正文阶段说明修改的模块。

footer 是可选的，用来记录对应因为这些更改而可以关闭的 issue，如 `Close #12345`

提交 PR 之前, 请确保你的更改可以成功在本地编译.

与大多数项目一样, review 通过之后请将你诸多commits rebase 成为一个 commit.

触发 CI 请 comment ```cicheck```.
  
若发现测试错误, 可查看 CI 对应错误测试.

CI 检查点有:
  
  1. 更改代码风格检查(cpplint)
  2. 分支覆盖率检查(可能会失败)
  3. 单元测试 (若失败请先尝试在本地运行对应失败测试)
  4. 混沌测试
  
重新 push 会触发 CI, 若暂时无反应, 请耐心等待, 测试处于排队中.

若 CI 不稳定, 可 comment ```recheck```, 重新触发 CI.

## 社区交流

目前 Curve 社区有多个沟通交流的渠道，请根据自己的需求选择合适高效的沟通方式：

- [**GIthub Issue**](https://github.com/opencurve/curve/issues): Curve 项目功能、性能等相关问题请优先选择该渠道，这样便于问题的持续追踪和研发同学的及时回复。
- [**Curve FAQ**](https://github.com/openCURVE/CURVE/wiki/CURVE-FAQ): 主要根据 Curve User Group中的常见问题整理，涵盖了大部分用户关心的问题，包括 Curve 的产品定位，应用场景，应用现状和开源目标等。在了解 Curve 项目过程中可查看常见问题来加深了解。
- [**Curve 论坛**](https://ask.opencurve.io/): Curve 论坛同样是重要的沟通渠道，在这 Curve 团队会发布技术文章和大家进行技术问题的探讨，如果你有关于 Curve 项目的问题可以在这里发布讨论话题。
- **Curve 微信公众号**: OpenCurve，可关注 Curve 微信公众号，我们每周都会发布文章，除了技术文章还会有 Curve 的应用进展和大新闻，便于大家了解 Curve 的现状。对于发布的内容有疑问或想了解的内容可以通过私信反馈。
- **slack**: cloud-native.slack.com，channel #project_curve
- **Curve User Group**: 为了便于大家即时的沟通，Curve User Group 目前为微信群，由于群人数过多，需要先添加OpenCurve_bot微信，再邀请进群。在用户群里大家可以自由的沟通关于 Curve 和存储相关的话题，对于存在问题也可以较为即时的得到反馈。

<img src="docs/images/curve-wechat.jpeg" style="zoom: 75%;" />
