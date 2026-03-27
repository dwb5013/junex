# 因子完成度

这份文档用于跟踪股票分析系统当前的因子建设进度。

建议和下列文档配套阅读：

- `docs/factor_definitions.md`
  - 负责字段定义、公式、口径约束
- `docs/factor_completion.md`
  - 负责完成度、可用性、下一步开发顺序

## 当前概览

目前已经落地的核心因子表有 4 张：

- `analytics.price_action_features`
- `analytics.market_industry_linkage_features`
- `analytics.flow_structure_features`
- `analytics.fundamental_event_features`

当前系统已经具备四层能力：

1. 描述股票本身的价格行为
2. 描述股票相对市场和行业的位置
3. 描述当天成交背后的资金结构
4. 描述最近财报、指引、分红等事件状态

## 完成度总表

| 因子组 | 当前状态 | 已完成 | 近期可增强 | 理想版仍缺 |
|---|---|---|---|---|
| 价格行为因子 | 高 | 绝对收益、相对 TOPIX、相对行业、K 线结构、跳空、量价冲击、真实波动、滚动波动率、涨跌停标记 | 均线状态、52 周位置、突破/回撤类指标 | 更完整的趋势状态标签、长期位置因子 |
| 市场与行业联动因子 | 中高 | 市场收益状态、行业收益状态、市场宽度、行业宽度、行业相对强弱 percentile、`beta_20`、`beta_60`、`topix_close_vs_ma20`、`industry_close_vs_ma20` | `ma5_vs_ma20`、市场 regime 标签、全市场横截面排名、residual alpha | 更完整的 regime 分类、行业 beta、剔除市场后的 alpha 拆解 |
| 资金结构因子 | 中 | `breakdown` 买卖盘拆解、行业 `short_ratio`、`margin_alert` 风险字段、`short_sale_report` 压力字段 | 滚动均值、异常分位、持续性标签、结构状态压缩 | squeeze 风险状态机、延续/衰竭规则、更强的空头压力解释层 |
| 基本面事件因子 | 中 | 财报事件窗口、下一次财报日期、forecast revision、分红变化、事件冲突标记 | 事件强度分数、surprise 分类、分类型 `DocType` 标签压缩 | `fins/details` 关键边际变化、事件后行为解释、财报 surprise 归因 |
| 标签与评估层 | 很低 | 还未正式实现 | `next_ret_1d`、`next_up_1d`、基础回测表 | 分 regime 评估、IC/lift、按因子拆解贡献、每日预测 scorecard |
| 规则引擎 / 评分层 | 很低 | 还未正式实现 | 基于现有三组因子的 `bullish / neutral / bearish` 基线规则 | 多因子加权评分、置信度校准、解释性 score composition |
| LLM 解释层 | 低 | 已能导出 JSON bundle 和 prompt 给外部 LLM | 固定 schema、统一 prompt、模型输出对比 | 产品内 provider 集成、版本化、评估闭环、多模型比较 |

## 已实现因子组

### 1. 价格行为因子

输出表：

- `analytics.price_action_features`

作用：

- 描述股票当天以及近 3/5/20 个交易日的价格行为
- 判断当前走势更像趋势延续、反弹修复还是波动放大
- 衡量相对大盘、相对行业的强弱

当前覆盖字段：

- 绝对收益：
  - `ret_1d`, `ret_3d`, `ret_5d`, `ret_20d`
- TOPIX 基准：
  - `topix_ret_*`, `excess_ret_*`
- 行业基准：
  - `industry_ret_*`, `industry_excess_ret_*`
- K 线结构：
  - `intraday_range_pct`
  - `body_ratio`
  - `upper_shadow_ratio`
  - `lower_shadow_ratio`
  - `close_position`
- 跳空：
  - `gap_pct`
- 量价冲击：
  - `volume_shock_20d_inclusive`
  - `value_shock_20d_inclusive`
- 波动：
  - `true_range`
  - `mean_true_range_5`
  - `mean_true_range_20`
  - `logret_vol_5`
  - `logret_vol_20`
- 极端标记：
  - `hit_limit_up`
  - `hit_limit_down`

当前判断：

- 这组因子已经足够支撑日度复盘和次日方向的第一层判断
- 目前最大的增强空间不在“再多堆几个技术指标”，而在“状态标签化”

### 2. 市场与行业联动因子

输出表：

- `analytics.market_industry_linkage_features`

作用：

- 把股票走势放到市场环境和行业环境里解释
- 区分市场 beta、行业跟随、个股 alpha
- 判断今天是广度驱动还是个股异动

当前覆盖字段：

- 市场状态：
  - `topix_ret_1d`
  - `topix_ret_5d`
  - `topix_close_vs_ma20`
- 行业状态：
  - `industry_ret_1d`
  - `industry_ret_5d`
  - `industry_close_vs_ma20`
- 相对行业收益：
  - `industry_excess_ret_1d`
  - `industry_excess_ret_5d`
- 市场宽度：
  - `market_up_ratio`
  - `market_new_high_20d_ratio`
  - `market_volume_up_ratio`
- 行业宽度：
  - `industry_up_ratio`
  - `industry_new_high_20d_ratio`
  - `industry_volume_up_ratio`
- 行业内强弱排名：
  - `industry_strength_pct_1d`
  - `industry_strength_pct_5d`
  - `industry_strength_pct_20d`
- 市场敏感度：
  - `beta_20`
  - `beta_60`

当前判断：

- 这组因子已经能有效区分：
  - 趋势延续 vs 局部反弹
  - 行业龙头表现 vs 普通跟涨
- 下一步最有价值的是把这些原始状态转成明确的 regime 标签

### 3. 资金结构因子

输出表：

- `analytics.flow_structure_features`

作用：

- 分析当天买盘和卖盘分别由什么结构构成
- 区分现物买、信用新规买、信用返済买、非融资空卖等不同来源
- 加入行业空卖背景以及稀疏但高价值的风险字段

当前覆盖字段：

- 交易总额：
  - `total_buy_va`
  - `total_sell_va`
  - `total_trade_va`
- 买盘拆解：
  - `long_buy_share_of_buy_va`
  - `margin_buy_new_share_of_buy_va`
  - `margin_buy_close_share_of_buy_va`
- 卖盘拆解：
  - `long_sell_share_of_sell_va`
  - `short_nomargin_share_of_sell_va`
  - `margin_sell_new_share_of_sell_va`
  - `margin_sell_close_share_of_sell_va`
- 净流向代理：
  - `net_cash_va`
  - `net_margin_new_va`
  - `net_margin_close_va`
- 行业空卖背景：
  - `SellExShortVa`
  - `ShrtWithResVa`
  - `ShrtNoResVa`
  - `sector_short_ratio`
- 日々公表信用风险：
  - `margin_alert_sl_ratio`
  - `margin_alert_long_out_chg`
  - `margin_alert_short_out_chg`
  - `margin_alert_pub_date`
- 大额空头压力：
  - `short_sale_pos_to_so`
  - `short_sale_prev_ratio`
  - `short_sale_ratio_change`
  - `short_sale_calc_date`

当前判断：

- 这组因子已经足够进入日常 LLM 分析 bundle
- 但还没有形成成熟的“结构状态分类”与“持续性判断”

### 4. 基本面事件因子

输出表：

- `analytics.fundamental_event_features`

作用：

- 描述最近一次财报、分红、指引修正等事件在当前交易日的状态
- 判断股票是否仍处于财报后事件窗口中
- 为次日方向分析提供“事件驱动”层证据

当前覆盖字段：

- 财报事件状态：
  - `is_earnings_day`
  - `latest_earnings_date`
  - `days_since_earnings`
  - `trading_days_since_earnings`
  - `next_earnings_date`
  - `days_to_next_earnings`
  - `earnings_event_window_1d`
  - `earnings_event_window_3d`
  - `earnings_event_window_5d`
- 财报摘要属性：
  - `latest_doc_type`
  - `material_change_subject`
  - `chg_by_as_rev_flag`
  - `chg_no_as_rev_flag`
  - `chg_ac_est_flag`
- 指引修正：
  - `latest_forecast_op`
  - `latest_forecast_net`
  - `latest_forecast_eps`
  - `prev_forecast_op`
  - `prev_forecast_net`
  - `prev_forecast_eps`
  - `forecast_op_revision_pct`
  - `forecast_net_revision_pct`
  - `forecast_eps_revision_pct`
  - `forecast_revision_direction`
- 分红事件：
  - `is_dividend_announcement_day`
  - `latest_dividend_date`
  - `days_since_dividend_announcement`
  - `latest_dividend_rate`
  - `prev_dividend_rate`
  - `dividend_revision_pct`
  - `dividend_revision_direction`
- 事件总结：
  - `guidance_positive_flag`
  - `guidance_negative_flag`
  - `fundamental_event_conflict_flag`

当前判断：

- 这组因子已经可以进入日常分析和 LLM bundle
- 但仍是第一版，重点是“事件是否发生、方向如何”，还不是“事件强度评分”

## 推荐开发顺序

从当前状态继续往下做，建议顺序如下：

1. 标签与评估层
2. 规则引擎 / 评分层
3. LLM 解释层产品化
4. 第二版基本面事件因子
5. 市场与行业 regime 补强

原因：

- 现在系统已经知道：
  - 股票怎么走
  - 它相对市场和行业处于什么位置
  - 当天背后的资金结构是什么
- 现在也已经知道：
  - 最近财报、分红、指引事件处于什么状态
- 目前最缺的不是“继续堆更多输入因子”
- 最缺的是：
  - 事件解释
  - 结果验证
  - 把因子系统变成稳定的判断系统

## 当前可用性判断

### 已可用

- 收盘后日度复盘
- 相对强弱分析
- 对外导出 JSON 给 LLM 做解释
- 基于四组因子的初步次日倾向判断

### 部分可用

- 资金结构的系统化解释
- 置信度表达
- 多因子联合判断

### 尚未就绪

- 完整的次日预测框架
- 分 regime 回测
- 稳定的生产级评分模型
- 自动评估闭环

## 维护建议

- 当新增字段时，先更新 `factor_definitions.md`
- 当新增一整组能力或一张新因子表时，再更新 `factor_completion.md`
- 如果某个因子组从“低”变成“中”，应同时更新：
  - 完成度总表
  - 推荐开发顺序
  - 当前可用性判断
