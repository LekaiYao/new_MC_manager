# new_MC_manager

这个目录用于试验更高自动化程度的 MC CRAB 管理流程。

设计目标：

- 作为独立项目维护，将来可单独建立 GitHub 仓库
- 与外部 `CMSSW_*` 工作区协作，但不把这些 `CMSSW` 目录纳入项目本身
- 保留旧版 `CrabTask_manager.py`/`CrabTask_large_submission_handler.py` 风格接口
- 增加结构化状态记录和统一 driver 入口

## 当前阶段

已实现第 1 阶段：

- `python3 pipeline_driver.py check <STEP>`
  - 调用与 `CrabTask_manager.py` 相同的状态检查逻辑
  - 保留原有 `log/` 和 `txt/` 输出
  - 额外写入 `state/pipeline_state.json`
- `python3 pipeline_driver.py report`
  - 从 `state/pipeline_state.json` 读取并输出结构化摘要

## 运行前提

- 已进入合适的 `CMSSW` 环境
- 命令行可直接使用 `crab`
- 与本目录同级存在外部 `CMSSW_*` 工作区
