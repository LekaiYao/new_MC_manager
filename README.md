# new_MC_manager

这个目录用于试验更高自动化程度的 MC CRAB 管理流程。

设计目标：

- 作为独立项目维护，将来可单独建立 GitHub 仓库
- 与外部 `CMSSW_*` 工作区协作，但不把这些 `CMSSW` 目录纳入项目本身
- 保留旧版 `CrabTask_manager.py` / `CrabTask_large_submission_handler.py` 风格接口
- 增加结构化状态记录和统一 driver 入口

## 本地配置

需要本地创建 `.local/v.json`，文件不进入 Git：

```json
{
  "voms_password": "111111"
}
```

## 当前可用命令

### 1. 旧式按 step 检查

```bash
python3 pipeline_driver.py check GEN
```

### 2. 增量检查当前活跃样本

```bash
python3 pipeline_driver.py check-active
```

作用：

- 只检查每个样本当前最新的未完成步骤
- 对已经 `ready_for_next_step` 或 `workflow_complete` 的样本跳过重复检查
- 自动进入对应 `CMSSW` 的 `src` 目录
- 自动执行 `cmssw-el7`、`cmsenv`、`voms-proxy-init`/proxy 复用、`crab status`
- 完成判定规则：`finished >= 95%` 且 `transferring == 0`

### 3. 生成样本状态表

```bash
python3 pipeline_driver.py table
```

输出：

- `state/pipeline_table.csv`
- 每个 step 列只有在该步骤满足完成判定后才填入 `output dataset`
- 未完成步骤留空

### 4. 查看当前状态摘要

```bash
python3 pipeline_driver.py report
```

## 文件

- `config.json`：公共流程配置，例如终点步骤、阈值、各 step 对应的 CMSSW 路径
- `.local/v.json`：本地密码文件，不进 Git
- `state/pipeline_state.json`：结构化样本状态
- `state/pipeline_table.csv`：可读表格
- `log/pipeline_events.jsonl`：事件日志
