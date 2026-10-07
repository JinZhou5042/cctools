#!/usr/bin/env python3
"""Generate a final internal handoff from terminal experiment and audit records."""
import hashlib,json,statistics
from pathlib import Path
P=Path(__file__).resolve().parents[1];R=P/'results';ROOT=P.parent
names=['compute-campaign-v4','distributed-campaign-v4','preloaded-campaign-v1','data-pipeline-v1','preloaded-data-v1']
campaigns={n:json.loads((R/n/'summary.json').read_text()) for n in names}
audit=json.loads((R/'artifact-audit.json').read_text())
if audit['status']!='PASS' or any(s['status']!='PASS' for s in campaigns.values()):raise RuntimeError('final handoff requires terminal campaigns and passing audit')
lines=['# DataVine paper handoff','',
'本轮已交付完整 LaTeX 研究稿、PDF、8 张实测/架构图、实验脚本和可审阅源码补丁。**这是已有实测证据的研究稿，还不能声称已经达到投稿就绪。**','',
'- [论文 PDF](build/paper.pdf)；[LaTeX 入口](paper.tex)。',
'- [复现实验与构建](README.md)；[本轮源码补丁](provenance/this-work-source.patch)。',
'- [逐项审计](results/artifact-audit.json)；[投稿前证据要求](experiments/next-evidence.md)。','',
'## 实验状态','', '| Campaign | Accepted / Planned | Status |','|---|---:|---|']
for n,s in campaigns.items():lines.append(f"| {n} | {s['passed']} / {s['planned']} | {s['status']} |")
lines += ['', '上述五组共 '+str(sum(s['passed'] for s in campaigns.values()))+' 个试验；每个都校验完整任务身份和跨配置结果一致性。另有 24 个内存/召回压力试验、6 个源路径试验、一次包含三次实际 worker 移除的 1,024 任务故障试验，以及 22/22 DataVine 回归和 generic TaskVine serverless 验证。','',
'## 实际结论','',
'明确预加载同一组依赖时，128-task 科学控制图上，DataVine elastic 相对 stock TaskVine 的中位数加速为 2.02–3.35x，相对增加固定并发的 TaskVine 为 1.74–3.71x。固定 DataVine 窗口常常同样快，因此不能把收益全归于弹性控制。',
'', '1,024-task / 1 MiB 数据流水线的预加载中位数：DataVine elastic 4.736 s、TaskVine 6.222 s、TaskVine+4C 6.071 s。冷启动流水线中 DataVine 明显更慢；负面结果全部保留。',
'', '进程诊断观测到 Python fork 子进程阻塞在 NFS open/RPC 等待。冷/预加载两组使用不同分配和 CPU 型号，不能把两组之间的比例当作 preload 的配对因果效果。预加载的计时仍可能包含 worker 连接后尚未完成的库启动。',
'', '## 本轮实现','',
'- 修复 dense worker pool 反复选择不兼容 worker、饿死召回目标任务的活性缺陷。原失败远程形状修复后通过，新增确定性回归。',
'- 增加可选择 eager/deferred 输入准备的实验控制，以及默认关闭的逐 attempt 时间戳。',
'- 增加私有依赖预加载 executor，使用已有路径 override；生产 executor 未被本轮改写。',
'- 修复故障实验对 transaction log 的依赖；当前版本重跑完成 3 次移除、18 个任务重提交、1,042 次物理提交、1,024 次唯一逻辑完成。',
'- 实验 worker 明确限制 CPU affinity，避免把 Condor CPU shares 误当物理 CPU 上限。','',
'## 投稿判断','',
'最能立住的主线是 dispatch / data preparation / execution admission 的分离，以及它们与数据生命周期、召回和结果持久化的事件顺序。不能声称首创 data controller、peer transfer 或 overcommit。',
'', '仍需补齐具有代表性的数据集上的完整科学应用、保留物理节点的规模实验、完整控制路径归因，以及 prepared-node 的最近基线。当前脚本与各项 acceptance 条件已经明确；没有把未运行的项目写成通过。详见 experiments/next-evidence.md。','',
'## 范围与复现','',
'原有脏 checkout 保留；源码补丁相对于开始时的脏文件生成，而非把历史 diff 都算作本轮工作。48 个其余已有脏文件哈希未变。没有无关源码被修改或删除。最初根目录 make 曾重建少量无关忽略对象文件，随后停止并限定构建范围；不能声称那些对象文件完全没动。',
'', '原始内部日志含站点路径和主机信息，不能直接作为匿名公开 artifact。论文文本通过匿名性、页数、引用和布局检查。未提交、未发布。','']
(P/'HANDOFF.md').write_text('\n'.join(lines))
paths=[ROOT/'taskvine/src/tools/datavine_executor',ROOT/'taskvine/src/tools/datavine_workflow',ROOT/'taskvine/src/worker/vine_worker']
(P/'provenance/final-runtime-fingerprints.json').write_text(json.dumps({str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in paths},indent=2)+'\n')
print('Wrote final handoff')
