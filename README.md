# new_MC_manager

用于管理 MC CRAB 工作流的自动检查、推进和重提。

## 核心命令

```bash
python3 pipeline_driver.py check-active
```
只检查每条 lineage 当前最新的未完成任务。脚本会进入对应 `CMSSW` 目录，执行 `cmssw-el7`、`scramv1 runtime -sh`、`voms-proxy-init`/proxy 复用、`crab status`。

```bash
python3 pipeline_driver.py submit-next
python3 pipeline_driver.py submit-next --execute
```
前者只做下一步提交规划，后者执行真实 `crab submit`，并在成功后更新 `state/pipeline_state.json`。

```bash
python3 pipeline_driver.py resubmit
python3 pipeline_driver.py resubmit --execute
python3 pipeline_driver.py resubmit --lineage <lineage_id>
```
前者只做重提规划，后者执行真实 `crab resubmit -d ...`。`--lineage` 可限制到单条 lineage。

```bash
python3 pipeline_driver.py table
python3 pipeline_driver.py report
python3 pipeline_driver.py check <STEP>
```
分别用于生成状态表、查看简要摘要、手动检查单个 step。

## 核心判据

- `ready_for_next_step`
  - `finished > 95%`
  - `publication done > 95%`
  - `|finished - publication_done| <= 0.1%`
  - 当前 step 已有可用于下一步的 `output dataset`
- `workflow_complete`
  - 满足 `ready_for_next_step` 的完成判据
  - 且当前 step 已是 `terminal_step`
- `resubmit` 候选
  - 只看每条 lineage 当前最新任务
  - 当前不能进入下一步
  - `publication done + failed > 95%`

## 本地配置

脚本在需要时会交互式执行 `voms-proxy-init --rfc --voms cms --valid 192:00`。

可选地创建 `.local/exclusions.json`，将某些 lineage/sample/request/project 屏蔽出自动链路：

```json
{
  "excluded_lineages": ["example_lineage"],
  "excluded_samples": ["example_sample"],
  "excluded_request_names": ["example_request"],
  "excluded_crab_projects": ["example_project"]
}
```

命中的链不会进入 `check-active`、`submit-next`、`resubmit`、`table` 的自动视图。
