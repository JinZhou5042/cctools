# Million-task throughput campaign

This campaign profiles and optimizes the one-workflow native C runtime with
one physical FunctionCall per logical task. The headline result is a repeated
local mean of 22,771.3 service tasks/s and a repeated Condor 8x16 mean of
28,705.2 service tasks/s.

- Human analysis: `REPORT.md`
- Machine summary: `summary.json`
- Result visualization: `throughput.png`
- Exact result documents: `raw/results/`
- Manager profile: `raw/profiles/`
- Condor service and factory logs: `raw/logs/`
- Integrity manifest: `SHA256SUMS`

Every accepted final million-task result reports exactly 1,000,000 physical
submissions and completions. No requested output, persistence job, durable
data file, or workflow recovery record is present in the final configuration.
