# new_MC_manager

这个目录用于试验更高自动化程度的 MC CRAB 管理流程。

## 常用命令

### 1. 检查当前自动链路中的最新任务

```bash
python3 pipeline_driver.py check-active
```

作用：

- 只检查每条 lineage 当前最新的未完成任务
- 自动进入对应 `CMSSW` 的 `src` 目录
- 自动执行 `cmssw-el7`、`scramv1 runtime -sh`、proxy 初始化/复用、`crab status`
- 完成判据使用当前代码逻辑：`finished > 95%`、`publication done > 95%`、且两者差值 `<= 0.1%`
- 已手动屏蔽的链不会进入自动检查

### 2. 查看下一步提交计划

```bash
python3 pipeline_driver.py submit-next
```

作用：

- 只做 dry-run，不会真实提交
- 列出哪些 lineage 已经可以进入下一步
- 列出哪些 lineage 被阻塞，以及阻塞原因

### 3. 真实提交下一步

```bash
python3 pipeline_driver.py submit-next --execute
```

作用：

- 对当前 `ready_for_next_step` 的 lineage 执行真实 `crab submit`
- 提交前自动改写目标 step 目录下的 `crab3_Config.py`
- 成功提交后立即更新 `state/pipeline_state.json`
- 已手动屏蔽的链不会进入自动提交

### 4. 生成当前链路状态表

```bash
python3 pipeline_driver.py table
```

输出：

- `state/pipeline_table.md`
- 每条 lineage 一行
- 每个 step 一列
- 只有满足完成判定的步骤才填 `output dataset`
- 已手动屏蔽的链不会出现在表格里

### 5. 查看简要状态摘要

`check-active` 的终端输出会同时显示 `finished`、`publication_done`、`transferring`、`ready`、`complete`。

```bash
python3 pipeline_driver.py report
```

### 6. 手动按 step 做一次检查

```bash
python3 pipeline_driver.py check GEN
```

作用：

- 对单个 step 跑一次 `crab status`
- 保留按 step 的传统日志输出

## 判据说明

当前代码中，单步状态分两层：

- `completed`：`finished > 95%`、`publication done > 95%`、且 `|finished - publication_done| <= 0.1%`
- `ready_for_next_step`：满足 `completed`，并且当前 step 已经有可用于下一步的 `output dataset`


## 本地配置

需要本地创建 `.local/v.json`，文件不进入 Git：

```json
{
  "voms_password": "111111"
}
```

可选地创建 `.local/exclusions.json`，把某些任务链从自动检查/提交中手动屏蔽掉：

```json
{
  "excluded_lineages": ["leyao_v1_set1"],
  "excluded_samples": ["leyao_v2_test1"],
  "excluded_request_names": ["MC2018_SIM_leyao_v2_test1"],
  "excluded_crab_projects": ["crab_MC2018_SIM_leyao_v2_test1"]
}
```

命中任一条件的链不会进入 `check-active`、`submit-next`、`table` 的自动视图。

当前轻量测试阶段，本地 `.local/exclusions.json` 已用于暂时屏蔽：

- `leyao_v1_set1`
- `leyao_v1_set2`

## 设计目标

- 作为独立项目维护，将来可单独建立 GitHub 仓库
- 与外部 `CMSSW_*` 工作区协作，但不把这些 `CMSSW` 目录纳入项目本身
- 增加结构化状态记录和统一 driver 入口
- 以 lineage 为单位做自动检查与自动推进

## 文件

- `config.json`：公共流程配置，例如终点步骤、阈值、各 step 对应的 CMSSW 路径
- `.local/v.json`：本地密码文件，不进 Git
- `.local/exclusions.json`：本地屏蔽列表，不进 Git
- `state/pipeline_state.json`：结构化样本状态
- `state/pipeline_table.md`：可读表格
- `log/pipeline_events.jsonl`：事件日志
