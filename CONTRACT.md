# 遥测入仓存储访问升级

本项目及 vendor 内的两代 SDK 均为本题独立编写，不是 Azure/ADLS 实现或原项目事故复现。只依赖 Python 标准库和 SQLite；完整源码即离线依赖。保留 vendor、schema_v1.sql、samples.py、scenarios.json 和 regression.py 现有断言，允许追加测试。schema_v2.sql 是目标结构，不含迁移实现。

## 已有业务与调用
每条记录为大端两个有符号 64 位整数 (observed,value)，固定16字节；ordinal从0起。记录顺序就是文件顺序，不按observed排序，也不做值去重。
Ingestor(db,world).run(task_id,hook=None) 返回已提交字节位置；hook是只限关键字的可选参数。rows(task_id) 返回按ordinal排序的 (ordinal,observed,value) 元组列表。db可为路径或已有SQLite连接。未知任务抛KeyError；EOF不足16字节抛ValueError，完整记录前缀保留。run没有持久完成标志，已完成任务再run返回相同位置且不重复入仓。
ingest.py负责记录解释及提交；storage.py负责字节交付。已成功返回的字节不等于已提交记录。旧链路已原子提交单条记录与其末尾位置，但没有SDK2凭据生命周期或目标任务适配。
run(task_id,hook=fn) 在每条记录事务内调用fn('before_commit',following)，成功提交后调用fn('after_commit',following)。保留这两个观察点及现有参数，异常原样传播。回调可在真实子进程中调用os._exit，以验证重开。

## 完整旧格式和样本
schema_v1.sql完整定义旧库：meta仅一行版本1；tasks(task_id,signed_url,etag,committed)；warehouse(task_id,ordinal,observed,value)。committed是已提交完整记录的末尾字节位置。
signed_url格式为mock://容器/百分号编码的对象键?sig=签名，完整解码规则见vendor.service.locator；ETag另存于任务，不能从路径当前版本重新推断。sig只是旧授权，不能作为新版对象身份。
python3 samples.py old.sqlite 创建真实旧库。也可用samples.seed(sqlite3.connect(':memory:'))。任务resume已提交[(0,100,12),(1,101,-4)]且位置32；empty同对象、无入仓行、位置0。原对象E1完整记录为[(100,12),(101,-4),(102,0),(103,19),(104,7)]，总长80。无异常时resume续读须得到五行、位置80；empty独立入仓，不能与resume共享断点。旧URL、ETag及全部历史行可由这个冻结构造器重建，无需二进制下载。
samples.world构造源服务；samples.from_script('scenarios.json')载入公开事件脚本。events的键为从1计数的读取调用号，值为依序执行的事件列表：replace用hex载荷新增版本并改变同路径当前版本，保留原版；drop只删除指定etag；fault的after表示改写至多该字节数后抛ReadInterrupted。replace/drop在该读取授权与版本查找之前执行。renew_fail_at为签发失败的逻辑时刻列表；max_read为SDK2每次成功返回的字节上限；old_expires为旧sig失效时刻。每次实际读取调用结束时钟加1，包括EOF及失败；打开和签发不推进时钟。测试也可在操作之间显式修改world.now。
scenarios.json在第二读替换当前路径为E2并注入3字节失败，时刻2签发失败，旧签名在时刻3失效；E1仍存在。升级后resume本次run应自动续读E1至80，五行仍为上述原版；不承诺新实现的精确读取调用数量。另测drop E1时须抛VersionGone，已提交前缀及断点保留，不能接入E2。

## SDK差异
SDK1 Client(world).open(signed_url,etag,offset) 返回read(count)->bytes流，成功返回才交付字节；失败无调用方缓冲改写。SDK1不会续签。
SDK2 Client(world).open(container,key,credential,if_match=etag,offset=n,length=None) 仅接受独立定位与Credential，返回readinto(buffer)->成功字节数流。length可限制一个区间，也可省略以读至EOF。非零请求成功短读不代表EOF，只有返回0表示该区间或对象结束。
SDK2流绑定打开时的凭据，换证不会修改已开流；readinto异常可改写缓冲并移动流内位置，但没有成功字节数，异常调用的内容不得入仓。
World.issue()签发不可变Credential(token,refresh_at,expires_at)，分别为当前时刻+2、+4。now>=expires_at失效；时钟只从公开服务取得，不用真实睡眠。issue在renew_fail_at时抛RefreshFailed。除上述公开接口、world.now与world.trace外，适配代码不得访问服务的对象字典、tokens、current、事件表或流内pos；不得绕过SDK取数据或自行构造凭据。
首次读取需取得新凭据；读取期间每次底层open/readinto前检查刷新时刻，到refresh_at应尝试签发，失败但旧证尚有效时继续读取，过期后不得发起底层读取。签发成功后须在后续读取中使用新证，不能仅缓存到过期；允许关闭旧流后按范围或位置重开。有限次ReadInterrupted须在本次run内恢复；VersionGone立即向调用方传播；无法取得有效凭据时传播RefreshFailed，不要求无限等待。无异常注入时须完成，不能以反复重开空读规避进度。
凭据刷新、流重开都必须携带任务原ETag，原版本不存在时保留断点并报错，禁止静默改绑当前版本。成功字节交付位置、未提交记录残片与持久提交位置须区分；进程退出丢弃未提交残片，重开从持久位置继续。载荷缓冲须有与文件大小无关的固定上界，不得先缓存整文件再入仓。

## 真实升级与兼容
提供migration.upgrade(db,*,hook=None)，成功返回None。在维护窗口单工作者下，把旧任务和入仓行迁入schema_v2.sql的目标表；format2仅一行版本2作为切换标记。旧tasks、warehouse、meta完整保留为只读审计，旧签名地址不再用于目标读写。
upgrade提交前调用hook('before_commit')；回调抛错或进程退出时，旧库内容不变且目标不可见。迁移表、复制数据和版本标记共同提交；重开可重试；已升级调用无副作用。不得在已有连接上用隐式提前提交的脚本执行方式破坏该边界。
保留Ingestor构造、run、rows和观察点；升级后，包括升级前已构造的Ingestor对象，均从目标表读取/提交，且仅经SDK2访问源。迁移后不得读取旧业务表来补算结果，审计原文不删除、不改写。无需支持旧进程二进制或并发工作者。
任务身份、ETag、已入仓行及committed原样保留，定位从旧URL迁为独立container/object_key。仓库记录与已提交字节位置原子推进；提交前后终止重开不漏不重。已有位置32不是流对象当前位置，也不是已读取总字节数。

## 交付核验
保留正常业务回归，补齐独立原始字节与入仓行对照，验证旧库真实升级、刷新失败与过期、异常缓冲、原版本保留/删除及提交前后真实子进程终止重开；不能把模拟抛异常当作进程终止证据。迁移失败验证也须检查旧库仍可用。提交简短的版本绑定、读取失败和提交边界说明。
.pointwise-runtime.json是可执行清单：version为1，kind为cli，setup为准备命令的argv数组列表，verify和demo各是一个argv数组；执行器不使用shell。现有verify运行旧回归，demo显示真实旧前缀、续读行和访问记录。交付时更新verify使其核验旧回归和本次升级，demo展示由旧库升级后经过SDK2的换证续读、结果与断点，不只打印成功。入口参数与新增核验代码一并提供。
