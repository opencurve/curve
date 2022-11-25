[中文版](Community_Guidelines_cn.md)


## Overview

This article aims to help community developers to get the full picture of the Curve project and better participate in the development and evolution of the Curve project. This article will describe how to better participate in the development of the Curve project from the perspective of community participants.

## How to learn about Curve

The premise of participating in an open source project is to understand it, especially for a large and complex project such as Curve, it is difficult to get started. Here are some information to help those who are interested in the Curve project:

- [Curve Overview](https://github.com/opencurve/curve/tree/master/docs)
- [Curve Architecture](http://www.opencurve.io/)
- [Curve Deploy](https://github.com/opencurve/curveadm/wiki)
- [Curve Design docs](https://github.com/opencurve/curve/tree/master/docs/cn)
- [Curve Code  Analysis](https://github.com/opencurve/curve/wiki/Curve源码及核心流程深度解读)
- [Curve FAQ](https://github.com/openCURVE/CURVE/wiki/CURVE-FAQ)

Through the study of the above materials, I believe that you already have an overall understanding of the Curve project, and there may also be some curiosity and doubts. At this time, you can deploy a Curve experience environment, which is conducive to a more intuitive perception of the Curve system. If you encounter a problem or want a new feature, you can track the relevant code. In the process, it is easy to understand the relevant modules. This is lots of contributors completed their first contributions.

The Curve community has multiple [communication channels](#Comminication), and there will be an online community meeting every two weeks. The regular meeting will synchronize the recent progress of the Curve project and answer your questions. You are also welcome to participate in the Curve community regular meeting. We will communicate at the meeting, so that we can quickly answer your questions, improve our understanding of the Curve project and synchronize what we are doing at this stage.

## How to participate in Curve

After you have a certain understanding of the Curve project, if you are interested, you can choose the starting point to participate in the Curve. You can choose from the following aspects:

- Start by selecting a interested one from the [Issues](https://github.com/opencurve/curve/issues) of the Curve project. You can pay attention to issues with the good_first_issue tag which we have assessed as relatively good starters.
- Based on a certain understanding of the Curve project, you can also choose from [Roadmap](https://github.com/opencurve/curve/wiki/Roadmap).
- Selecting form [Issues](https://github.com/opencurve/curveadm/issues) and [Roadmap](https://github.com/opencurve/curveadm/issues/92) of  Curve Operation and maintenance tools [Curveadm](https://github.com/opencurve/curveadm). Here are [quick start guidelines](https://github.com/opencurve/curveadm/wiki/develop)。
- In addition to the existing issues, you are also welcome to submit issues that you have discovered or new features you hope for and resolve them.
- You can pay attention to the *TODO* in the existing Curve code, most of which are code logic optimization and features to be supplemented, choose the ones you are interested in and raise relevant issues to follow up and try to solve.

For commit messages:

A good commit message needs to contain the following elements:

1. What is your change? (required)
2. Why this change was made? (required)
3. What effect does the commit have? (Optional)

The footer is optional and is used to record issues that can be closed due to these changes, such as `Close issue-12345`

Explain what changes have been made to the submitted PR: performance optimization? Fix bugs? add feature? and why. Finally, describe the impact of the following modifications, including performance and so on. Of course, for some simple modifications, the reasons and effects of the modifications can be ignored. Try to follow the following principles in the message:

- Summarize the function and role of PR
- Use short sentences and simple verbs
- Avoid long compound words and abbreviations

Please follow the following format as much as possible when submitting:

```
[type]<scope>:<description>
<BLANK LINE>
[body]
<BLANK LINE>
[footer]
```

type can be one of the following types:

- build: Affects system builds and external dependencies
- ci: Affects continuous inheritance related functions
- docs: Documentation related changes
- feat: Add new features
- fix: A bug fix
- perf: Performance improvement
- refactor: Refactor related code without adding functionality or fixing bugs
- style: Modifications that do not affect the meaning of the code, only modify the code style
- test: Modifications related to unit testing

The first line indicates that the title should be kept within 70 characters as much as possible, explaining the modified module and content, multiple modules can be represented by `*`, and the modified module is explained in the text stage.

The footer is optional and is used to record issues that can be closed due to these changes, such as `Close #12345`

## How to submit a proposal

After you find a point of interest, you can discuss it through an issue. If it is a small bug-fix or a feature point, you can start development after a brief discussion. Even if it is a simple problem, it is recommended to communicate with us first, so as to avoid a deviation in the understanding of the problem or a problem with the solution, and useless efforts are made.

If the work to be done is more complicated, you need to write a detailed design document and submit it to curve/docs. The existing design solutions in this directory are for your reference. A good design plan should clearly write the following points:

- Background description
- what problem to solve
- Scheme design (scheme description, correctness and feasibility), preferably including the comparison of several schemes
- Compatibility with existing systems
- Concrete realization of the scheme

It is also recommended to communicate with us before starting the plan writing. If you are not sure about the feasibility of the plan, you can provide a simple plan description first, and then refine and develop it after our evaluation.


## How to submit a PR

Once you've finished writing the code, you can submit a PR. Of course, if the development has not been completed, you can submit PR first in some cases. For example, if you want to let the community take a look at the general solution, you can raise the price of PR after completing the code framework.

[Quick build Curve development environment](https://github.com/opencurve/curve/blob/master/docs/en/build_and_run_en.md)

[Build and run Curve tests](https://github.com/opencurve/curve/blob/master/docs/en/build_and_run_en.md#test-case-compilation-and-execution)

For PR we have the following requirements:

- The CURVE coding standard strictly follows the [Google C++ Open Source Project Coding Guide](https://google.github.io/styleguide/cppguide.html), please follow this guide as well Submit your code.
- The code must have test cases, excluding documentation, unit tests (incremental lines cover more than 80%, and incremental branches cover more than 70%); integration tests (merge statistics with unit tests, and meet the unit test coverage requirements).
- Please fill in the description of the PR as detailed as possible,  associate with the relevant issues, and the PR commit message can clearly see the resolved issues. After submitting to the Curve master branch, Curve CI will be triggered automatically. It is necessary to ensure that the CI is passed, and the Jenkins username and password of the CI is netease/netease, if the CI fails to run, you can log in to the Jenkins platform to view the reason for the failure.
- After the CI is passed, the review can start, and each PR needs to get at least two LGTMs of Committer/Maintainer before merging.
- PR code requires a certain amount of comments to make the code easy to understand, and all comments and review comments and replies are required to be in English.

## Comminication

At present, the Curve community has multiple communication channels, please choose an appropriate and efficient communication method according to your needs:

- [**GIthub Issue**](https://github.com/opencurve/curve/issues): For questions related to the function and performance of the Curve project, please choose this channel first, which is convenient for continuous tracking of problems and timely responses from R&D students.
- [**Curve FAQ**](https://github.com/openCURVE/CURVE/wiki/CURVE-FAQ): It is mainly organized according to the common problems in the Curve User Group, covering the issues that most users care about, including Curve's product positioning, application scenarios, application status and open source goals. Check out the Frequently Asked Questions to learn more about the Curve project.
- [**Curve forum**](https://ask.opencurve.io/): The Curve forum is also an important communication channel, where the Curve team will publish technical articles and discuss technical issues with everyone. If you have questions about the Curve project, you can post a discussion topic here.
- **Curve WeChat public account**: OpenCurve，You can subscribe to Curve's WeChat official account. We will publish articles every week. In addition to technical articles, there will be Curve's application progress and big news, so that everyone can understand the current situation of Curve. If you have any questions or want to know about the published content, you can give feedback through private messages.
- **slack**: cloud-native.slack.com，channel #project_curve
- **Curve User Group**: In order to facilitate instant communication with everyone, the Curve User Group is currently a WeChat group. Due to the large number of people in the group, it is necessary to add OpenCurve_bot WeChat first, and then invite into the group. In the user group, everyone can freely communicate about Curve and storage-related topics, and get immediate feedback on problems.

<img src="docs/images/curve-wechat.jpeg" style="zoom: 75%;" />