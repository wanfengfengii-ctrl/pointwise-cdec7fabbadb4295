# 遥测入仓 SDK1→SDK2 迁移与边界说明

## 版本绑定

- `format2` 表存在且恰有一行 `version=2` 才视为已升级；这是唯一的持久切换标记。
  `Ingestor` 每次 `run`/`rows` 都先检查它，所以**升级前构造的 Ingestor 对象**在升级后
  自动走 `tasks2`/`warehouse2` + `Storage2`，无需重建。
- 升级后旧 `meta`/`tasks`/`warehouse` 只读保留：不删除、不改写、不再用于任何读写。
  甚至把旧 `warehouse` 行清空、把旧 `signed_url` 改坏，也不影响新结果（有测试覆盖）。
- 任务身份：`ETag` 与 `committed` 原样迁入 `tasks2`；定位由旧签名 URL 经
  `vendor.service.locator` 一次性解码为独立的 `container`/`object_key`。`sig` 不参与
  新对象身份，也不会在运行期再解析旧 URL。
- 每次 SDK2 `open` 都显式传 `if_match=任务原ETag`。版本只由持久 ETag 决定，绝不从
  路径当前版本重新推断。

## 读取与换证

- 凭据生命周期在 `storage.Storage2`：首次读取前必须 `world.issue()`；每次底层
  `readinto` 前检查 `refresh_at`，到期即尝试换证。
- 换证失败但旧证 `now < expires_at`：继续用旧证读取（不会贸然中止）。签发成功后
  一定会被使用——SDK2 流绑定打开时的凭据，所以按已交付偏移关旧流并用新证重开；
  不会只把新证缓存到过期。`now >= expires_at` 时不再发起任何底层调用，向上传播
  `RefreshFailed`。
- 流重开（换证、异常恢复）都使用**已交付位置** `delivered`，而不是流内部 `pos`，
  也不是 `committed`（已提交位置可能落后于已交付位置），更不是“已读取总字节数”。
- `ReadInterrupted`：SDK2 可能已改写缓冲并推进流内位置，但无成功字节数，因此该次
  缓冲内容一律丢弃、关流、按 `delivered` 重开。有限次中断在本次 run 内自动恢复；
  `MAX_INTERRUPTS` 仅防止永久故障无限重放。
- 只有 `readinto` 成功返回 `count>0` 的字节才计入 `delivered` 并交给 `Ingestor`；
  成功返回 0 才表示 EOF（短读不等于 EOF）。`VersionGone` 立即传播，断点保留。
- 载荷缓冲是**单个固定 4096 字节 bytearray**，与对象大小无关；`Ingestor` 只保留不足
  16 字节的残片。

## 提交边界

- `Ingestor.run` 仍是逐条原子提交：一条 `warehouse2`(或 v1 `warehouse`) 行与
  `committed` 前进 16 字节在同一事务内，`hook('before_commit', following)` 在事务内、
  `hook('after_commit', following)` 在提交后。回调异常原样传播。
- 未提交残片只在内存中；进程退出即丢弃，重开从持久 `committed` 继续——恰好一条记录
  不漏不重。真实 `os._exit` 子进程测试覆盖“提交前退出”（行与位置都回滚到 32）与
  “提交后退出”（行与位置都保留到 48，重开续读到 80 且无重复序号）。
- `run` 无持久完成标志：已完成任务再次 `run` 从 `committed` 打开，读到 0 字节即返回
  相同位置，不重复入仓。

## 迁移原子性

- `migration.upgrade(db, *, hook=None)`：单工作者、`BEGIN IMMEDIATE`，DDL（拆分为逐条
  `execute`，不用会隐式 COMMIT 的 `executescript`）、`tasks2`/`warehouse2` 复制、
  `format2` 标记全部在同一事务内；`hook('before_commit')` 返回后才 COMMIT。
- 回调抛错或真实进程 `os._exit`：ROLLBACK，旧库原样（表和数据逐行相同），目标表不可见；
  旧链路仍可继续 run，重试 `upgrade` 可成功。
- 已升级后再次调用：直接 ROLLBACK 返回 `None`，无任何数据变动。
- 打开连接若已处于事务中会直接报错，避免隐式提交破坏边界。
- 迁移前做一致性校验：`meta` 必须恰为 1；每个任务 `committed%16==0` 且有 ETag；
  复制后再次逐行比对 `etag`/`committed` 与全部 `warehouse` 行。

## 已知边界

- 不支持旧进程二进制；不支持多工作者并发（维护窗口假设）。
- `MAX_INTERRUPTS=1000` 是安全阀：超过后把最后一次 `ReadInterrupted` 原样抛出，
  断点保持，可在下次 run 重试。
- 源对象若在 EOF 后继续增长，本次 run 不会主动追读新增内容；下次 run 从持久位置继续。
