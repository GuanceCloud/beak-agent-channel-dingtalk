# 钉钉审批模板

`beak-approval-card.json` 是钉钉卡片设计器的编辑源，基于官方审批示例的组件结构生成。请在独立测试应用中导入，在设计器中预览并发布，再把发布的模板 ID 填入 IM 新增或编辑界面的「审批卡片模板 ID」。编译产物由设计器生成；本地 JSON 检查不代表导入、渲染或发布已通过。

变量契约：`title`、`summary`、`state`、`pending`，以及 `approve/reject/revise/submit` 各自的 `_label`、`_token`、`_visible`。所有变量均为公共字符串。按钮仅在 `state=pending` 且对应 `_visible=true` 时显示；按钮状态必须为 `normal`，不能套用官方示例中“已同意/已拒绝”终态展示按钮的 `disabled` 状态。动作类型为服务端回传（`actionType=request`），参数名为 `beak_action`，值绑定对应 `_token`，`actionId` 分别为 `beak_approve`、`beak_reject`、`beak_revise`、`beak_submit`。不要将 `_token` 当成授权凭据，服务端还会校验账号、会话、操作人及版本。

应用须开通卡片创建、投放和更新权限；连接使用 Stream 订阅 `/v1.0/card/instances/callback`，用户 ID 类型固定为 1（staff userId）。模板不支持转发。终态更新全量覆盖卡片变量并移除按钮。

配置顺序：在目标机器人的同一企业中创建「标准卡片」模板并关联应用（编辑源顶层 `type=standard`、`mode=card`；选择「消息卡片」会提示「卡片类型不符合」），导入本文件，检查变量与回传请求配置，再用设计器预览待审批、批准、拒绝三种状态。确认待审批按钮可点击、终态不显示可操作按钮后再发布，并将模板 ID 保存到对应的 Beak 钉钉渠道。模板采用状态标题、中文状态标签、独立审批内容区和横向操作按钮；待审批显示发起人授权范围提示，终态隐藏按钮和待审批提示。具体审批事项由 `summary` 动态填入，一个模板可供不同命令和计划确认复用。模板发布后不可修改；需要变更时创建新模板，验证后更新渠道中的模板 ID。

本地回归读取真实 `beak-approval-card.json`，将 `InteractionData` 输出与模板变量、按钮可见条件、可操作状态及 `beak_action` 回调绑定一起校验，覆盖四种动作和所有终态。运行 `GOWORK=off go test . -run TestDingtalkApprovalTemplateContract`。这只验证 SDK 与模板编辑源的契约，不替代导入、原生渲染、客户端点击回调和消息状态更新的真实平台验收。

来源：[钉钉官方审批示例](https://github.com/open-dingtalk/dingtalk-card-examples/tree/2201b867795e273c0b1ac6d908d72cbc486e19bb/examples/审批模板)；`SingleButton.status` 的 `dynamicSelect/fixed/normal` 格式参照同版本的[事件链配置演示](https://github.com/open-dingtalk/dingtalk-card-examples/tree/2201b867795e273c0b1ac6d908d72cbc486e19bb/examples/事件链配置演示)。

2026-10-26 使用「金磊测试」在真实群聊验证了官方审批示例模板的发送、同意、拒绝及卡片终态更新。实测 Stream 回调的 `spaceType` 为 `im`，`spaceId` 为原始会话 ID；它与投放参数 `openSpaceId=dtv1.card//IM_GROUP.<会话 ID>` 的格式不同。SDK 同时接受该实际回调格式，仍将卡片 ID、会话 ID、操作人和版本交由后端校验。这项结果仅证明官方模板交互能力。随后在驻云的「Beak审批测试」应用下，自有模板已通过标准卡片导入、原生预览和发布；待审批预览显示具体命令及批准/拒绝按钮，批准和拒绝预览均隐藏按钮。这些设计器结果不代表真实卡片回调或 Beak 业务执行通过，后两项仍需单独验收。

2026-10-26 样式回归：驻云「Beak审批测试」已发布 V3 标准模板，并在 QA 的默认权限渠道完成真实私聊验证。客户端待审批显示完整测试命令和并排批准/拒绝按钮；点击后 Beak 审批记录分别变为 approved/rejected，原卡片更新为中文终态并隐藏按钮，模型继续回复。设计器也验证了待审批、批准、拒绝三种状态。实机曾出现空白，重新打开钉钉窗口后恢复完整显示，尚不能断言其根因。QA 资源盘存在归档写入失败，网页批准后的工具条目仍显示待执行；模型报告执行成功不能替代独立工具执行凭据，此项需与环境问题分开验收。
