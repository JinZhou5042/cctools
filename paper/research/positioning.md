# DataVine 面向 SC 的研究定位与论文故事

日期：2026-09-06。读者：DataVine 作者团队。范围：现有 Scheduler / Data Controller 分离、数据解析与传输、Worker 弹性执行三个机制；以 SC、OSDI、SOSP、NSDI、SIGCOMM、HPDC 等会议的原始论文为主，补充直接相关的 IPDPS、CCGrid、eScience 与 SC workshop/poster。后者明确标注，不作为 SC 主会论文。

本报告提出论文定位和实验设计，不宣称已经证明优于所有相关系统。论文机制按发表版本比较；当前软件行为另引官方文档。DataVine 实现按本次只读代码检查判断，历史实验按归档 JSON 判断，没有重新运行性能或故障实验。THman 仅获得官方摘要，细节比较保留为 OPEN。

## 1. 直接结论

**保留现有架构主线。最有说服力的定位是：DataVine 重新划分任务分发、数据解析和实际执行的决策边界，使逻辑工作流推进不必同步等待副本准备与持久化完成，再由 Worker 用当前资源压力控制有效并行。**

这比“首次把 scheduler 和 data controller 分开”更具体，也比“peer transfer + overcommit”更难被简单的功能先例否定。它仍是需要比较实验支撑的贡献候选，不是已获证实的首创性结论。

推荐研究问题：

> 在数据密集型动态科学工作流中，将任务分发与节点本地数据准备、执行资源准入分开，何时能够降低协调开销并提高有效计算吞吐？什么情况下，局部性损失、传输拥塞和资源压力会抵消收益？

推荐一句话 positioning：

> **DataVine separates task dispatch from data resolution and execution admission, allowing workers to prepare and execute work under an independent data lifecycle and local resource feedback.**

研究应围绕一条因果链组织：中央运行时能够提前分发逻辑就绪的任务；消费侧按实际可用副本解析输入；Worker 保留廉价任务队列并控制真实执行。评估必须说明每条边界为何必要，以及它们在什么工作负载下共同产生收益。

“所有组件各自独立”不是目标。依赖、引用、结果提交和失败仍然需要协调；目标是减少不必要的同步等待，并把需要新鲜信息的决策放到合适的位置。

## 2. 当前实现能支持哪些主张

以下依据 [production contract](../../DATAVINE_PRODUCTION.md)、[project map](../../DATAVINE_MAP.md) 和本次代码检查。

| 主张 | 本次确认的实现 | 论文边界 |
|---|---|---|
| Scheduler / Data Controller 分离 | Scheduler 管理依赖、逻辑完成、dispatch/retry；Controller 管理副本、持久化、丢失与 GC 状态 | 属于职责与协议边界；不能仅凭模块分离宣称独立进程扩展或去中心化 |
| 完成与持久化解耦 | 成功 attempt 释放后继；dispatch 不等待 Controller persistence | 后继实际执行仍要求输入可用；动态 frontend 消费 requested control results 仍可能等待其持久化 admission |
| 消费侧解析数据 | Agent 检查本地副本；未命中则向 Controller resolve，获得具体副本/回退来源后获取数据 | 不能描述为 Worker 完全自主掌握所有副本或独立决定全局最优路径 |
| 多路径 | 生产中为 local-first、peer-first、Controller backup fallback | SharedFS 与按文件大小切换是实验替代路径；当前未实现三种路径之间的在线性能择优 |
| 提前排队与延迟准备 | DataVine fork call 没有可用执行 library slot 时，保留廉价描述符，推迟 Agent prepare 和 sandbox | 不等于已经存在独立可调的 transfer window 与 compute window；需要测量 staging 等待如何影响执行机会 |
| Worker 弹性 execution window | CPU、runnable、RSS 反馈；执行窗口从 2C 开始；队列容量为 8 倍 per-core capacity | queue depth、execution window、实际 runnable processes 是三个不同量 |
| recall | 只撤回尚未开始的任务，带 generation 校验 | 不迁移正在运行的进程，不宣称任意外部副作用 exactly-once |
| 数据存活与恢复 | Worker-local intermediate、异步 backup、按 DataID/generation 解析与失效处理 | Controller `/tmp` 不提供 Controller-host 故障后的跨主机耐久性；open workflow 可能保留很多未来消费者需要的数据 |

代码入口：

- [finish_logical_attempt](../../taskvine/src/datavine/vine_datavine_workflow_runtime.c)：调用 `vine_datavine_scheduler_mark_done`，也更新 pending consumers，并在合适条件下调用 Controller 的 mark-dead。**因此不能说 workflow runtime 完全没有 per-file 工作或跨组件生命周期协调。**
- [Worker process_ready_to_run_now](../../taskvine/src/worker/vine_worker.c)：先检查执行 library 机会，再调用 Agent prepare；输入未就绪时返回等待，之后再做资源匹配与 sandbox readiness。
- [Agent prepare_generated / resolve_one](../../taskvine/src/worker/vine_datavine_agent.c)：本地检查、Controller resolve、peer 或 Controller 来源。
- [Worker adaptive window](../../taskvine/src/worker/vine_worker.c)：250 ms 采样；CPU 饱和且 runnable 压力连续出现时回退；内存有独立 backoff 与预测检查。

历史 elastic REPORT 中仍出现“3C hard cap”措辞；当前代码和 production contract 明确 3C 是反馈阈值。研究采用后者。本报告不修改历史证据。

## 3. 最接近的论文，以及真正需要回答的区别

### 3.1 TaskVine 系列：最重要的直接对照

**TaskVine: Managing In-Cluster Storage for High-Throughput Data Intensive Workflows，WORKS / SC-W 2023。** §3.3 明确指出传输管理与 scheduler 紧密耦合；Manager 维护副本和正在进行的传输，选择来源并协调传输。peer-first、节点存储和异步传输都已存在。因此 DataVine 的差异应是将副本解析及相关生命周期责任移出通用调度路径，而不是首次 peer transfer。[作者全文](https://ccl.cse.nd.edu/research/paper/taskvine-works-2023.pdf)

**Reshaping High-Energy Physics Applications for Near-Interactive Execution Using TaskVine，SC 2024。** §IV.B 描述 Worker-local intermediate、Manager 的数据局部性放置与 peer 获取。论文已经把硬件、数据放置、调度和函数执行的改进串成科学应用故事。DataVine 应接着回答：这些优化之后，剩下的调度/数据协调与资源准入问题是什么。[作者全文](https://ccl.cse.nd.edu/assets/paper/pdf/reshaping-sc-2024.pdf)

**Liberating the Data Aware Scheduler to Achieve Locality in Layered Scientific Workflow Systems，eScience 2025。** §IV 将依赖信息传递给 TaskVine，以组的形式向 Worker 提交仍然独立的任务，并允许队列中的依赖稍后满足。其实现选择组内无并行；不能把 grouping 一律说成任务融合或失去单任务语义。链式图上 DataVine 必须与它比较，并测试分支图、阶段变化和独立数据生命周期的额外价值。[作者全文](https://ccl.cse.nd.edu/research/paper/liberating-escience-2025.pdf)

**判断：** 这是最有力也最公平的研究起点。使用已经启用 temporary intermediates、peer transfer 和合适函数执行方式的现代 TaskVine，才能证明 DataVine 改变了控制边界的价值。

### 3.2 Hoplite、Pocket、ExoFlow：限制过宽的“解耦”主张

**Hoplite，SIGCOMM 2021。** §3 使用消费端 Get、ObjectID directory 和动态副本选择；支持局部读取、完整/部分副本以及流水传输。消费时查找数据、独立数据服务和动态 peer 选择已经有直接先例。DataVine 应强调其与 DAG dispatch、数据存活、Worker admission 的关系，不能声称自己首次实现这些传输原语。[会议全文](https://conferences.sigcomm.org/sigcomm/2021/files/paper/3452296.3472897.pdf)

**Pocket，OSDI 2018。** 将 control、metadata、data planes 分开，针对短暂中间数据在多种存储技术之间进行放置与资源调节。它说明“独立数据服务 + 多层存储 + 资源反馈”本身已经是成熟研究方向。DataVine 的目标更具体：计算节点上的文件型中间结果如何与逻辑任务推进和执行准入配合。[会议全文](https://www.usenix.org/system/files/osdi18-klimovic.pdf)

**ExoFlow，OSDI 2023。** §1、§3–4 分离执行与恢复，使用引用、任务语义 annotations 和可选择的 checkpoint 策略，并协调 nondeterministic / externally visible outputs。它直接限制“首次不等 checkpoint 推进后继”的说法，但不会自动否定 DataVine 对 dispatch/data/admission 的不同划分。我们应声明 Worker-loss/replay 和 requested-output 的具体边界，不与其较强的 exactly-once DAG 契约混比。[会议全文](https://www.usenix.org/system/files/osdi23-zhuang.pdf)

**判断：** DataVine 不能仅靠“独立 Controller”取胜，需要说明同样具有独立存储或恢复服务的 runtime 为什么仍会有我们观察到的等待，以及新边界如何减少它。

### 3.3 WOW、WONDERS、THman：最应补充的新近 workflow 对照

**WOW，CCGrid 2025。** §III 的 assignment 约束只允许任务分配到具有全部输入的 prepared node；通过显式和推测 copy operations 预先放置数据。§IV.C 的实现记录副本和 copy 状态，并在 copy/task 完成等事件后重新调度。它与 DataVine 最清楚的差异是 **prepare-before-assignment 与 dispatch-before-local-readiness**。这不是说 WOW 不能重叠计算与传输，而是二者在何时允许向某节点分配任务上不同。[全文](https://arxiv.org/html/2503.13072v1)，[大学归档及 CCGrid 出版记录](https://eprints.gla.ac.uk/350601/)

**WONDERS，SC 2025 poster，非主会论文。** 将 WOW、PONDER、SCALE 组合，涵盖数据放置、内存预测、CPU 需求调整；poster 已明确使用解耦与协同的表述。DataVine 不能依靠“三机制组合”四个字主张新颖性。候选区别是当前 Worker aggregate pressure 驱动 execution window，与基于相似已完成任务预测 resource requests 的区别，以及 dispatch 前是否要求本地输入准备完成。[官方 poster](https://sc25.supercomputing.org/proceedings/posters/poster_files/post183s2-file2.pdf)

**THman，Towards Highly Compatible IO-aware Workflow Scheduling on HPC Systems，SC 2024。** 官方摘要确认在线 compute/I/O co-scheduling、多层存储、HCF heuristic 和 HPC batch scheduler 兼容。**全文未获得**，因此不能说它缺少 Worker feedback 或异步 staging。正式写 novelty 前需补齐全文逐项比较。[官方论文记录](https://sc24.supercomputing.org/proceedings/tech_paper/tech_paper_pages/pap727.html)

**判断：** 最有价值的对比轴是“以数据准备约束任务分配”与“允许提前分配、由 Worker 处理输入与执行压力”。需要测出优劣区间，而不是预设其中一种总是更好。

### 3.4 Apollo、Sparrow：提前 dispatch 及 recall 也有先例

**Apollo，OSDI 2014。** §3.4 将任务提前放入 server-local queues，再通过延迟修正处理不准确的分配；§3.5 通过 opportunistic execution 利用空闲资源。提前队列、修正和机会执行不是 DataVine 首创。差异需要落在廉价描述符、延迟 staging、数据生命周期和当前窗口反馈的具体交互上。[作者全文](https://www.cs.columbia.edu/~jrzhou/pub/osdi14-paper-boutin.pdf)

**Sparrow，SOSP 2013。** §3.5–3.6 的 late binding 是先排队 probe reservation，Worker 可执行时再获取具体任务。DataVine 提前绑定任务、延迟数据准备，语义不同。建议与 bounded-prefetch / pull dispatch 对照；不能把两者统称为相同的 late binding。[作者全文](https://people.csail.mit.edu/matei/paper/2013/sosp_sparrow.pdf)

### 3.5 IPDPS 2024、Wasabi、Svalinn：Worker overcommit 的边界

**Adaptive Task-Oriented Resource Allocation for Large Dynamic Workflows on Opportunistic Resources，IPDPS 2024，Phung 与 Thain。** §IV 使用已完成任务的资源记录进行在线 bucketing 与后续任务 allocation，支持估计不足后的调整。同组这项工作必须引用。DataVine 当前控制的是 aggregate execution window；二者的控制变量不同。应比较 resource prediction、Worker feedback 及其组合。[作者全文](https://ccl.cse.nd.edu/assets/paper/pdf/adaptive-ipdps-2024.pdf)

**Wasabi，Hierarchical Integration of WebAssembly in Serverless for Efficiency and Interoperability，NSDI 2026。** §3.3 已有资源感知本地 queuing/admission：利用 allocated memory 与 measured CPU，允许 overcommit。§6.4 实验选择固定 40% ratio，不能写成它已经在线求得最优 ratio。它直接否定“第一次按 CPU/内存决定并发”的说法。与 DataVine 的比较应限定为 policy / decision boundary，不能把 Wasm request isolation 与 native scientific tasks 当成同一运行成本。[会议全文](https://www.usenix.org/system/files/nsdi26-baqershahi.pdf)

**Svalinn，Overload Control in Large-Scale Servers with Multiple Resource Bottlenecks，OSDI 2026。** 分离吞吐 admission 与各瓶颈的 latency control，包括多种资源的控制机制。它提醒我们 CPU 低、RSS 低并不代表继续增加并发有利：内存带宽或其他瓶颈仍可能饱和。DataVine 的轻量反馈可以作为工程选择，但目前没有普遍稳定性或最优性证明。[会议全文](https://www.usenix.org/system/files/osdi26-pardeshi.pdf)，[官方会议记录](https://www.usenix.org/conference/osdi26/presentation/pardeshi)

**判断：** overcommit 最适合成为 DataVine 架构中的机制贡献，以科学 DAG 与数据等待条件下的作用来证明价值。仅凭 AIMD、CPU/RSS 阈值或 recall，很难独立构成强新颖性主张。

### 3.6 必须承认的更广泛背景

| 工作与会议 | 本次确认的相关事实 | 对定位的约束 |
|---|---|---|
| [Ray，OSDI 2018](https://www.usenix.org/system/files/osdi18-moritz.pdf) | §4.2.2 有每节点 local scheduler 与全局调度分工，并考虑队列和输入传输 | 不能将 Ray 描述为每个任务都必须经过单一中央调度器 |
| [Ownership，NSDI 2021](https://www.usenix.org/system/files/nsdi21-wang.pdf) | 基于 futures ownership 分散对象状态、引用和恢复责任 | DataID、ownership、lineage 或独立对象管理并非首次；DataVine 也不应冒称控制去中心化 |
| [Legion，SC 2012](https://theory.stanford.edu/~aiken/publications/paper/sc12.pdf) | logical regions 与 physical instances 分离，runtime mapper 决定计算和数据映射 | 逻辑数据身份与物理位置解耦已有 HPC 先例 |
| [Dynamic Control Replication，PPoPP 2021](https://research.nvidia.com/sites/default/files/pubs/2021-02_Scaling-Implicit-Parallelism/ppopp.pdf) | 分散动态依赖分析，保持隐式并行程序语义 | 当前无需为争取首创而转向泛泛的分布式控制架构 |
| [Parsl，HPDC 2019](https://arxiv.org/pdf/1905.02158) | 动态依赖图、多种 executor、弹性 provisioning 和数据管理 | Python dynamic DAG、executor 分层不是新贡献；2019 论文不代表今日 Parsl 的所有能力 |
| [CIEL，NSDI 2011](https://www.usenix.org/events/nsdi11/tech/full_paper/Murray.pdf) | 动态 task graph、引用、任务生成和恢复 | dynamic workflow 与 lazy replay 不能作为首创口号 |

此外，[当前 Ray 文档](https://docs.ray.io/en/latest/ray-core/tasks/nested-tasks.html#yielding-resources-while-blocked) 明确 `ray.get` 阻塞时释放 CPU、返回时重新获取。这个事实来自当前文档，不归因于本次未在 2018 论文中确认的细节。比较 Ray 时应允许恰当的 resource declarations，并区分 runtime-aware blocking 与普通应用 I/O。

## 4. 推荐的贡献结构

### 主贡献：重新划分三种决策，而不只是三个模块

建议贡献陈述如下。它们是作者可采用的设计描述；其中性能结论必须由后续实验补齐。

1. **A decoupled workflow execution contract.** Separate logical task dispatch from replica resolution and persistence completion, while maintaining explicit data lifetime and Worker-loss recovery semantics.
2. **Consumer-side data resolution integrated with deferred preparation.** Resolve DataIDs through current local, peer, or admitted Controller replicas, and avoid materializing queued work before an execution opportunity exists.
3. **Worker-local elastic execution with revocable queues.** Adapt execution concurrency to observed resource pressure while allowing never-started work to be reassigned without changing logical task granularity.

第二点目前是整个架构的重要机制，不宜包装为首个 adaptive transport algorithm。如果开发并验证真正的 online path selection，可以将其升级为独立算法贡献；否则要诚实使用 replica resolution / policy-based selection。

论文的实证贡献应贯穿这三项：量化中央协调与局部性之间的取舍，展示完整设计在哪些真实科学工作流上扩大了高效执行的范围。

### 不宜使用的主张

| 过强表述 | 建议替代 |
|---|---|
| 首次分离计算与数据 | 明确分离 dependency-driven dispatch 与 replica/persistence decisions |
| 首次动态选择 peer/local/sharedfs | 当前为消费侧副本解析；三路径性能择优属于待开发功能 |
| 首次 Worker oversubscription | 用当前资源反馈控制提前分发任务的执行窗口 |
| 完全消除 Scheduler 的数据管理开销 | 将副本选择和持久化完成移出 dispatch 前提；保留必要依赖和存活协调 |
| Scheduler 不再有瓶颈 | 数据相关协调减少后，逐任务 transport/status 仍有上限 |
| 80% 内存阈值确保永不 OOM | 已测试配置中的保护有效，未知峰值与多线程任务仍需评估 |
| 自适应始终优于最佳固定策略 | 接近合理固定配置的性能，并减少跨阶段手动调参；报告负收益情况 |
| 无需先验信息 | execution feedback 不要求 I/O/CPU 类型标签，但仍使用资源容量与 declared memory |
| 99.938% 网络流量下降 | 历史 pilot 的 generic Manager data-plane bytes 下降；总网络流量没有该结论 |

## 5. 论文故事：从科学问题到架构边界

### 开头应建立的矛盾

科学分析越来越由许多有依赖的文件/函数任务组成。阶段间任务长度、数据扇出和 I/O/CPU 比例变化，使单一固定并发和一次性的放置决策难以持续合适。论文应先用目标应用 trace 证明这些现象在我们的应用中真实存在。

已有系统通过节点缓存、peer transfer 和 locality-aware scheduling 降低数据搬运成本。但在具体实现中，数据准备和资源 reservation 仍可能影响任务何时分配、何时能真正执行。这里需要引用并精确限定 TaskVine 和 WOW 的策略，不泛指所有系统。

提出 insight：**任务在逻辑上可推进、输入在目标节点可使用、节点适合开启更多执行，是不同的判断，应该允许不同组件在不同时间做出。**

接着承认解耦产生的新问题：提前队列会使任务停留在不合适节点；超量执行可能放大 I/O 拥塞；未完成持久化的数据仍可能丢失。然后依次引出廉价可撤回队列、消费侧数据解析、Worker feedback 和独立生命周期契约。这样设计章节是在解决具体后果，而不是枚举功能。

### 建议标题

首选，准确覆盖当前设计：

> **DataVine: Decoupling Task Dispatch, Data Resolution, and Execution Admission for Scientific Workflows**

备选，强调应用目标：

> **DataVine: Sustaining Scientific Workflow Execution with Decoupled Data Management and Elastic Concurrency**

不要在已有版本标题中使用 optimal、autonomous routing、decentralized 或 exactly-once。最终是否突出 dynamic，应由 result-driven graph 的实证篇幅决定。

### 可用于 Introduction 的英文草稿

以下是待数据支撑的写作草稿；第一段的 workload 特征必须用目标应用测量支持，最后一段是评估计划，不是已经取得的结果。

> Data-intensive scientific workflows combine dependent computations whose data access patterns and resource demands change across execution stages. Efficient execution therefore requires both timely task dispatch and effective use of the resources that process and exchange intermediate results. A task's logical readiness, however, does not imply that its inputs are local or that a worker should immediately admit another computation.
>
> Existing systems provide important building blocks, including in-cluster caching, peer transfers, asynchronous data services, and adaptive resource management. The remaining question is how these mechanisms should interact. In systems that coordinate replica preparation with task assignment, data-management activity can constrain dispatch. Moving this activity toward workers creates a different challenge: queued work and changing I/O demand must be managed without excessive staging, contention, or stranded computation.
>
> We present DataVine, a workflow runtime that separates task dispatch, data resolution, and execution admission. The scheduler advances logical dependencies without waiting for Controller persistence. An independent data controller manages replica identity, lifetime, and recovery, while worker agents resolve inputs through available replicas. Workers retain inexpensive, revocable task descriptors and regulate execution concurrency using observed resource pressure. This organization preserves individual task attempts while allowing data preparation and execution policy to evolve independently of logical workflow progress.
>
> Our evaluation will isolate the effects of these decision boundaries from differences in data paths, task granularity, and durability. It will characterize when early dispatch improves scientific throughput, when locality or I/O contention limits its benefit, and whether worker feedback maintains performance across changing workflow phases.

最终投稿时，将最后一段替换为已完成实验的核心结果、范围和科学意义；不要把不同 workload、不同 timer 的最大值拼成一句结果。

### Related Work 的组织方式

以三个问题组织，而不是按系统名称逐个罗列：

- **Who coordinates task and data progress?** TaskVine、WOW/THman、Legion、Ray/Ownership、Parsl。讨论分发前提、信息归属、局部性取舍。
- **How are intermediate data resolved and recovered?** Hoplite、Pocket、ExoFlow。讨论对象引用、获取时机、存活与故障契约。
- **When does queued work consume execution resources?** Apollo、Sparrow、IPDPS resource allocation、Wasabi、Svalinn。讨论提前队列、绑定、反馈的控制变量。

WONDERS 作为直接组合先例引用并明确其 poster 身份。相关工作应承认重叠，再将区别落到事件顺序与测量结果；不能从论文未提及某功能推断系统绝无此功能。

## 6. 能让故事成立的实验

### 核心实验：比较决策边界，而不只是系统默认值

构造两个功能匹配的策略：

- **Prepared-node assignment：** 节点具备输入后才分配任务。
- **Early dispatch：** 逻辑就绪后可以提前分发，Worker 负责输入解析和后续执行。

二者使用相同内核、数据、副本策略、输出与恢复语义、计算资源，并记录预取是否允许及其成本。不要通过关闭对照组的 peer transfer 来制造差异。

在此基础上分离 dispatch queue depth、deferred preparation 和 execution window。功能存在依赖时，不强行运行语义无效的完整 2^3 组合；明确有效组合和缺失组合原因。

| 研究问题 | 对照 | 必须解释的观测 |
|---|---|---|
| 分离是否减少协调限制？ | 相同数据路径下的边界 A/B | ready-to-dispatch、Manager CPU、Controller CPU、metadata rate、完成处理成本 |
| 提前 dispatch 是否损害 locality？ | prepared-node、least-loaded early、允许的 locality/grouping baseline | peer bytes、重复读取、传输尾延迟、makespan |
| 延迟 staging 有何作用？ | eager staging、deferred staging、bounded pull/prefetch | sandbox 数、准备中的任务、内存/磁盘峰值、浪费的传输 |
| Worker feedback 是否值得？ | C/2C/4C 等固定 sweep、事后最佳固定、单 CPU 信号、完整反馈 | 阶段吞吐、收敛/振荡、最坏 slowdown、实际 runnable/RSS |
| 与历史 resource prediction 有何区别？ | per-task bucketing、aggregate feedback、二者组合 | 冷启动、phase change、预测错误、调参成本 |
| recall 是否有效？ | recall 开/关，晚到 Worker、异构负载 | queue residency、被撤回的未开始任务、尾部空闲、staging waste |
| 故障代价是否被隐藏？ | 相同 backup/journal 策略，Worker loss | 重算量、恢复时间、结果一致性、额外字节和资源时间 |

### 真实应用与规模

建议以一个 HEP/Coffea 类多阶段分析作为直接继承 TaskVine SC2024 的 anchor，再选一个文件密集的 fan-out/fan-in 工作流，以及一个由结果决定后续图结构的应用。具体应用以已有可运行、可验证科学结果的环境为准。三个同构 synthetic graphs 不能替代应用多样性。

合成实验用于解释因果，覆盖真实文件/网络 I/O、CPU、多线程数值库、memory bandwidth、突发 RSS、扇出与阶段变化。sleep 可隔离等待机制，但不能承担主要科学收益主张。

规模选择以能回答问题为准：固定问题强扩展、按资源增长的弱扩展、对象数与对象大小分别变化。中央 overhead 应报告全部协调组件总 CPU/RSS 和资源占用，不能仅把 Manager 工作搬到未计费 Controller。

### 应优先生成的五张图

1. **决策时序图：** 同一应用中 logical-ready、dispatch、prepare、execute、publish、persist 的时间线。特别区分数据 staging 等待与运行中的应用 I/O。
2. **有效资源时间分解：** useful compute、data wait、coordination、retry/duplicate waste，避免把 committed cores 当有效利用率。
3. **策略适用区域：** 在 task CPU / data transfer ratio、扇出、网络负载变化下，显示 early dispatch 相对 prepared-node 的胜负。
4. **机制消融：** 合法组合的 makespan 与资源开销，同时报告置信区间或原始重复分布。
5. **阶段变化与故障：** execution window、runnable、RSS、传输等待、吞吐随时间变化；展示恢复与过量并发的负面情况。

简单启发模型可解释为什么固定核心数并发不足：若每个活动任务平均计算时间 c、独立等待时间 w，保持 C 个核忙所需活动数近似 C(c+w)/c。但 w 会随并发和 I/O 拥塞变化，该式仅是直觉，不是控制器最优性或稳定性证明；也不应把执行前 staging 自动计入运行中任务的 w。

### 系统基线的优先级

1. 当前 TaskVine 的合理优化配置；同源码/环境、temporary intermediates、peer enabled，匹配 executor 和持久化。
2. TaskVine grouping / 相关 layered workflow 策略，尤其对链和局部性敏感应用。
3. Ray 或 Dask 的一个实际可维护强基线；允许正确的资源声明、缓存、spill 和适用的阻塞处理，不以未调优默认值代表系统上限。
4. WOW 风格 prepared-node policy 与 per-task resource prediction 的受控实现。若不是运行原系统，明确标为 policy-inspired baseline，不能用原系统名标注结果。
5. Wasabi/Svalinn/Pocket/ExoFlow 等视主张范围选择机制对照，不要求把每个异构系统全部移植才能写论文。

## 7. 当前实验能说什么，不能说什么

本节数值来自本次读回的归档 JSON；它们属于原实验版本，不是今天重新运行的结果。

| 证据 | 可以支持 | 尚不能支持 |
|---|---|---|
| [Data-intensive pilot](../../acceptance/data-intensive-fixed-ab-20260830.json)：3 对交替实验、2×4 Worker cores、1,024 tasks，paired median speedup 16.094 | 值得进一步剖析的强性能信号 | 不能归因于某一个机制，不能代表 full-scale，也不能直接迁移到今天默认 background backup 语义 |
| [Elastic JSON](../../acceptance/adaptive-window-20260903/summary.json)：6 类本地 workload、每类 3 次，elastic/best-fixed 为 98.11%–100.71% | 在测试配置内接近所测固定窗口最佳值 | 不是所有窗口或所有 workload 的全局最优；不是大规模科学应用结论 |
| [1M big pool JSON](../../acceptance/million-task-bigpool-20260903/summary.json)：32×16，27,069 service tasks/s | 精确物理任务计数和零负载 control ceiling | 无数据、无 backup、无 journal；不能证明数据密集型扩展，也没有显示增加到 32 Workers 提升吞吐 |
| [Dense crossover JSON](../../acceptance/storage-crossover-20260901/dense-summary.json)：路径中位数胜者改变 7 次 | 此环境和采样下不支持稳定的单一 size threshold | 不是在线 routing 算法，不证明所有环境都不存在 size threshold，也不是 peer/local/sharedfs 的全策略比较 |

特别注意：当前 TaskVine pilot harness 的 [temporary output 路径](../../acceptance/scripts/benchmark_data_intensive_taskvine.py) 已使用 `declare_temp` 与 `enable_temp_output`。不能说其所有 intermediate 都经过中央 Manager。还要继续分解 source staging/cache、sandbox、调用包装、IR registration、数据管理和 execution admission 的影响。

统计和 timer 边界必须明确：admission、graph registration、service、requested-result durability、background drain 分别计时。不能让一边等待 backup drain、另一边不等待；如果 durability 策略不同，应作为不同 trade-off 点报告。

## 8. 最值得增加的新功能

优先做 **data-path-aware preparation/admission**，以强化现有三个机制之间的联系。

当前 CPU/RSS 反馈不能区分“还有有用计算可启动”和“大家都在拥塞的同一条 I/O 路径上等待”。建议 Agent/Controller 暴露少量聚合状态：输入等待原因、各来源的传输排队与完成速率、已准备任务数量、活跃准备的字节量。Worker 据此限制新增 preparation，或优先利用已准备好的计算机会。

这是待开发设计。不能在当前论文实现章节中画成已经有两个独立的 I/O/CPU 控制窗口。首先测量现有 staging/admission 是否造成实际阻塞；没有证据时，不增加复杂控制器。

如果继续实现 local/peer/sharedfs 的在线择优，先定义每条候选路径的 eligibility：同一 DataID/generation、访问条件、数据完整性及所需耐久性。再比较实际等待和服务时间，避免把 volatile peer read 与 durable sharedfs publication 当成同一种动作。先测试轻量模型和抑制频繁切换的策略，不需要为新颖性引入无证据的 ML routing。

这项扩展能提出更强的问题：**数据路径决定新增任务会把压力放在哪里，execution admission 又会改变该路径的拥塞；两类决策如何用低协调成本配合？** 新颖性仍需与 Hoplite、THman、WONDERS 和多资源控制工作逐项对照。

## 9. 研究决策与剩余缺口

**建议选择现有三条主线继续推进，暂不转向可迁移执行域或全面分布式 Scheduler。** 这些新架构会引入不同的一组先例和恢复复杂度，尚无证据说明它们比把当前边界做深更适合本项目。

立即优先级：

1. 固定论文语义：三个状态转换、Controller/Worker 的具体决定权、backup/recovery 边界、queue 与 execution window 区别。
2. 对强 TaskVine 基线完成一次数据/控制/执行成本归因，检查历史大 speedup 的主要来源。
3. 做 prepared-node 与 early-dispatch 的功能匹配比较，再加入 admission/deferred-staging 消融。
4. 用真实应用的阶段变化与网络/存储压力决定是否开发 data-path-aware admission。
5. 补齐 THman 全文，并在投稿前针对新增主张再做一次最新文献核查。

可以捍卫的结论是“有具体、可检验的架构贡献候选”。本次研究不能证明世界首创，不能保证 SC 接收，也不能将既有 pilot 升格为完整评估。若合理 grouping/fixed-window 或 prepared-node 基线已经解释全部收益，就应收窄贡献；若完整设计在多个科学工作流中持续改善吞吐和资源效率，并能解释失败区间，现有架构足以成为有分量的投稿主线。

## 10. 来源范围与可复核性

本报告分析 20 项会议发表物：TaskVine WORKS2023、TaskVine SC2024、grouping eScience2025、Hoplite SIGCOMM2021、Pocket OSDI2018、ExoFlow OSDI2023、WOW CCGrid2025、WONDERS SC2025 poster、THman SC2024、Apollo OSDI2014、Sparrow SOSP2013、adaptive allocation IPDPS2024、Wasabi NSDI2026、Svalinn OSDI2026、Ray OSDI2018、Ownership NSDI2021、Legion SC2012、DCR PPoPP2021、Parsl HPDC2019、CIEL NSDI2011。不将 Ray 官方文档算成论文；每项来源的标题、会议、年份和访问链接见对应分析段落。

检索包括三条独立问题线：workflow/data-control ownership；local/peer/shared storage 和复制/获取时机；Worker oversubscription、queueing、resource feedback 与 recall。先按具体系统名和会议获取原始论文，再针对最接近的机制做全文定位，补查 2025–2026 工作。搜索词族包括 `TaskVine transfer management`、`workflow data placement dynamic`、`Hoplite Get directory`、`THman SC2024`、`WONDERS WOW PONDER SCALE`、`Apollo local queues opportunistic`、`adaptive task resource allocation`、`Wasabi overbooking`、`Svalinn admission`、`Legion mapping`、`ExoFlow checkpoint references`、`Parsl HPDC2019`。

停止原因：三个贡献轴均已有强原始来源和明确重叠边界，继续广泛搜集相似论文不太可能改变本次 positioning；保留 THman 全文、未来功能 novelty 和新实验作为具体缺口。这是有针对性的深度调查，不是穷尽所有会议的 systematic review。

访问状态：除 THman 仅官方摘要、WONDERS 本身为官方单页 poster 外，其余主张依据公开论文相关章节；并不声称逐页验证每篇论文的所有实验。高影响事实由主研究者再次读取原文或当前代码确认。日期采用论文/会议记录，未采用搜索结果中的抓取日期推断发表年份。

交付格式为 Markdown，检查标题、链接、表格和数值一致性；未生成或视觉检查 PDF。仅新增本 DataVine 研究报告，不修改既有运行时代码、实验产物或无关模块。
