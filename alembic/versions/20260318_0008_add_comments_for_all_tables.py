"""add comments for all tables

Revision ID: 20260318_0008
Revises: 20260318_0007
Create Date: 2026-03-18 01:15:00
"""
from __future__ import annotations

from alembic import op


revision = "20260318_0008"
down_revision = "20260318_0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        COMMENT ON TABLE market_data.equity_master_snapshot IS
        'J-Quants 上場銘柄一覧スナップショット。每行代表某个生效日期的一只上市证券主数据，主键为 (as_of_date, code)。数据来源 /v2/equities/master。';
        """
    )
    op.execute(
        """
        COMMENT ON COLUMN market_data.equity_master_snapshot.as_of_date IS
        '信息适用日期，对应 API 字段 Date。表示该行主数据在哪个日期生效。';
        """
    )
    op.execute(
        """
        COMMENT ON COLUMN market_data.equity_master_snapshot.code IS
        'J-Quants 返回的证券代码，对应 API 字段 Code。通常为 5 位代码；若查询时传入 4 位代码，API 可能返回对应普通股代码。';
        """
    )
    op.execute(
        """
        COMMENT ON COLUMN market_data.equity_master_snapshot.company_name IS
        '公司名称，对应 API 字段 CoName。';
        """
    )
    op.execute(
        """
        COMMENT ON COLUMN market_data.equity_master_snapshot.company_name_en IS
        '公司英文名称，对应 API 字段 CoNameEn。';
        """
    )
    op.execute(
        """
        COMMENT ON COLUMN market_data.equity_master_snapshot.sector17_code IS
        '17 业种代码，对应 API 字段 S17。可能值包括：1=食品，2=能源资源，3=建設・資材，4=素材・化学，5=医薬品，6=自動車・輸送機，7=鉄鋼・非鉄，8=機械，9=電機・精密，10=情報通信・サービスその他，11=電気・ガス，12=運輸・物流，13=商社・卸売，14=小売，15=銀行，16=金融（除く銀行），17=不動産，99=その他。';
        """
    )
    op.execute(
        """
        COMMENT ON COLUMN market_data.equity_master_snapshot.sector17_name IS
        '17 业种名称，对应 API 字段 S17Nm。是 sector17_code 的名称版。';
        """
    )
    op.execute(
        """
        COMMENT ON COLUMN market_data.equity_master_snapshot.sector33_code IS
        '33 业种代码，对应 API 字段 S33。可能值包括：0050=水産・農林業，1050=鉱業，2050=建設業，3050=食料品，3100=繊維製品，3150=パルプ・紙，3200=化学，3250=医薬品，3300=石油・石炭製品，3350=ゴム製品，3400=ガラス・土石製品，3450=鉄鋼，3500=非鉄金属，3550=金属製品，3600=機械，3650=電気機器，3700=輸送用機器，3750=精密機器，3800=その他製品，4050=電気・ガス業，5050=陸運業，5100=海運業，5150=空運業，5200=倉庫・運輸関連業，5250=情報・通信業，6050=卸売業，6100=小売業，7050=銀行業，7100=証券・商品先物取引業，7150=保険業，7200=その他金融業，8050=不動産業，9050=サービス業，9999=その他。';
        """
    )
    op.execute(
        """
        COMMENT ON COLUMN market_data.equity_master_snapshot.sector33_name IS
        '33 业种名称，对应 API 字段 S33Nm。是 sector33_code 的名称版。';
        """
    )
    op.execute(
        """
        COMMENT ON COLUMN market_data.equity_master_snapshot.scale_category IS
        '规模类别，对应 API 字段 ScaleCat。该值由 J-Quants 直接返回，表示 TOPIX 等规模分类。';
        """
    )
    op.execute(
        """
        COMMENT ON COLUMN market_data.equity_master_snapshot.market_code IS
        '市场区分代码，对应 API 字段 Mkt。可能值包括：0101=東証一部，0102=東証二部，0104=マザーズ，0105=TOKYO PRO MARKET，0106=JASDAQ スタンダード，0107=JASDAQ グロース，0109=その他，0111=プライム，0112=スタンダード，0113=グロース。';
        """
    )
    op.execute(
        """
        COMMENT ON COLUMN market_data.equity_master_snapshot.market_name IS
        '市场区分名称，对应 API 字段 MktNm。是 market_code 的名称版。';
        """
    )
    op.execute(
        """
        COMMENT ON COLUMN market_data.equity_master_snapshot.margin_code IS
        '貸借信用区分代码，对应 API 字段 Mrgn。可能值为：1=信用，2=貸借，3=その他。根据官方说明，该字段通常仅在 Standard / Premium 计划可用。';
        """
    )
    op.execute(
        """
        COMMENT ON COLUMN market_data.equity_master_snapshot.margin_name IS
        '貸借信用区分名称，对应 API 字段 MrgnNm。是 margin_code 的名称版。';
        """
    )
    op.execute(
        """
        COMMENT ON COLUMN market_data.equity_master_snapshot.source_api IS
        '数据来源 API 路径。当前固定为 /v2/equities/master。';
        """
    )
    op.execute(
        """
        COMMENT ON COLUMN market_data.equity_master_snapshot.fetched_at IS
        '本地抓取并写入数据库的时间戳，不是交易所公告时间。';
        """
    )
    op.execute(
        """
        COMMENT ON COLUMN market_data.equity_master_snapshot.raw_payload IS
        'J-Quants 原始返回 JSON，用于审计、排查和后续补充字段。';
        """
    )

    op.execute(
        """
        COMMENT ON TABLE market_data.equity_daily_bar IS
        'J-Quants 日线四本值表。每行代表某个交易日的一只证券日线行情，主键为 (trade_date, code)。数据来源 /v2/equities/bars/daily。';
        """
    )
    op.execute("COMMENT ON COLUMN market_data.equity_daily_bar.trade_date IS '交易日期，对应 API 字段 Date。';")
    op.execute("COMMENT ON COLUMN market_data.equity_daily_bar.code IS '证券代码，对应 API 字段 Code。';")
    op.execute("COMMENT ON COLUMN market_data.equity_daily_bar.open_price IS '调前开盘价，对应 API 字段 O。若当日无成交，可能为 NULL。';")
    op.execute("COMMENT ON COLUMN market_data.equity_daily_bar.high_price IS '调前最高价，对应 API 字段 H。若当日无成交，可能为 NULL。';")
    op.execute("COMMENT ON COLUMN market_data.equity_daily_bar.low_price IS '调前最低价，对应 API 字段 L。若当日无成交，可能为 NULL。';")
    op.execute("COMMENT ON COLUMN market_data.equity_daily_bar.close_price IS '调前收盘价，对应 API 字段 C。若当日无成交，可能为 NULL。';")
    op.execute("COMMENT ON COLUMN market_data.equity_daily_bar.upper_limit_flag IS '日通ストップ高フラグ，对应 API 字段 UL。可能值：0=不是涨停，1=涨停。';")
    op.execute("COMMENT ON COLUMN market_data.equity_daily_bar.lower_limit_flag IS '日通ストップ安フラグ，对应 API 字段 LL。可能值：0=不是跌停，1=跌停。';")
    op.execute("COMMENT ON COLUMN market_data.equity_daily_bar.volume IS '调前成交量，对应 API 字段 Vo。若当日无成交，可能为 NULL。';")
    op.execute("COMMENT ON COLUMN market_data.equity_daily_bar.turnover_value IS '成交额，对应 API 字段 Va。若当日无成交，可能为 NULL。';")
    op.execute("COMMENT ON COLUMN market_data.equity_daily_bar.adjustment_factor IS '调整系数，对应 API 字段 AdjFactor。例如发生 1:2 拆股时，权利落日可能为 0.5。';")
    op.execute("COMMENT ON COLUMN market_data.equity_daily_bar.adjusted_open_price IS '调整后开盘价，对应 API 字段 AdjO。';")
    op.execute("COMMENT ON COLUMN market_data.equity_daily_bar.adjusted_high_price IS '调整后最高价，对应 API 字段 AdjH。';")
    op.execute("COMMENT ON COLUMN market_data.equity_daily_bar.adjusted_low_price IS '调整后最低价，对应 API 字段 AdjL。';")
    op.execute("COMMENT ON COLUMN market_data.equity_daily_bar.adjusted_close_price IS '调整后收盘价，对应 API 字段 AdjC。';")
    op.execute("COMMENT ON COLUMN market_data.equity_daily_bar.adjusted_volume IS '调整后成交量，对应 API 字段 AdjVo。';")
    op.execute("COMMENT ON COLUMN market_data.equity_daily_bar.morning_open_price IS '前场开盘价，对应 API 字段 MO。官方说明该类前场字段通常仅 Premium 计划可用。';")
    op.execute("COMMENT ON COLUMN market_data.equity_daily_bar.morning_high_price IS '前场最高价，对应 API 字段 MH。';")
    op.execute("COMMENT ON COLUMN market_data.equity_daily_bar.morning_low_price IS '前场最低价，对应 API 字段 ML。';")
    op.execute("COMMENT ON COLUMN market_data.equity_daily_bar.morning_close_price IS '前场收盘价，对应 API 字段 MC。';")
    op.execute("COMMENT ON COLUMN market_data.equity_daily_bar.morning_upper_limit_flag IS '前場ストップ高フラグ，对应 API 字段 MUL。可能值：0=不是涨停，1=涨停。';")
    op.execute("COMMENT ON COLUMN market_data.equity_daily_bar.morning_lower_limit_flag IS '前場ストップ安フラグ，对应 API 字段 MLL。可能值：0=不是跌停，1=跌停。';")
    op.execute("COMMENT ON COLUMN market_data.equity_daily_bar.morning_volume IS '前场成交量，对应 API 字段 MVo。';")
    op.execute("COMMENT ON COLUMN market_data.equity_daily_bar.morning_turnover_value IS '前场成交额，对应 API 字段 MVa。';")
    op.execute("COMMENT ON COLUMN market_data.equity_daily_bar.morning_adjusted_open_price IS '前场调整后开盘价，对应 API 字段 MAdjO。';")
    op.execute("COMMENT ON COLUMN market_data.equity_daily_bar.morning_adjusted_high_price IS '前场调整后最高价，对应 API 字段 MAdjH。';")
    op.execute("COMMENT ON COLUMN market_data.equity_daily_bar.morning_adjusted_low_price IS '前场调整后最低价，对应 API 字段 MAdjL。';")
    op.execute("COMMENT ON COLUMN market_data.equity_daily_bar.morning_adjusted_close_price IS '前场调整后收盘价，对应 API 字段 MAdjC。';")
    op.execute("COMMENT ON COLUMN market_data.equity_daily_bar.morning_adjusted_volume IS '前场调整后成交量，对应 API 字段 MAdjVo。';")
    op.execute("COMMENT ON COLUMN market_data.equity_daily_bar.afternoon_open_price IS '后场开盘价，对应 API 字段 AO。官方说明该类后场字段通常仅 Premium 计划可用。';")
    op.execute("COMMENT ON COLUMN market_data.equity_daily_bar.afternoon_high_price IS '后场最高价，对应 API 字段 AH。';")
    op.execute("COMMENT ON COLUMN market_data.equity_daily_bar.afternoon_low_price IS '后场最低价，对应 API 字段 AL。';")
    op.execute("COMMENT ON COLUMN market_data.equity_daily_bar.afternoon_close_price IS '后场收盘价，对应 API 字段 AC。';")
    op.execute("COMMENT ON COLUMN market_data.equity_daily_bar.afternoon_upper_limit_flag IS '後場ストップ高フラグ，对应 API 字段 AUL。可能值：0=不是涨停，1=涨停。';")
    op.execute("COMMENT ON COLUMN market_data.equity_daily_bar.afternoon_lower_limit_flag IS '後場ストップ安フラグ，对应 API 字段 ALL。可能值：0=不是跌停，1=跌停。';")
    op.execute("COMMENT ON COLUMN market_data.equity_daily_bar.afternoon_volume IS '后场成交量，对应 API 字段 AVo。';")
    op.execute("COMMENT ON COLUMN market_data.equity_daily_bar.afternoon_turnover_value IS '后场成交额，对应 API 字段 AVa。';")
    op.execute("COMMENT ON COLUMN market_data.equity_daily_bar.afternoon_adjusted_open_price IS '后场调整后开盘价，对应 API 字段 AAdjO。';")
    op.execute("COMMENT ON COLUMN market_data.equity_daily_bar.afternoon_adjusted_high_price IS '后场调整后最高价，对应 API 字段 AAdjH。';")
    op.execute("COMMENT ON COLUMN market_data.equity_daily_bar.afternoon_adjusted_low_price IS '后场调整后最低价，对应 API 字段 AAdjL。';")
    op.execute("COMMENT ON COLUMN market_data.equity_daily_bar.afternoon_adjusted_close_price IS '后场调整后收盘价，对应 API 字段 AAdjC。';")
    op.execute("COMMENT ON COLUMN market_data.equity_daily_bar.afternoon_adjusted_volume IS '后场调整后成交量，对应 API 字段 AAdjVo。';")
    op.execute("COMMENT ON COLUMN market_data.equity_daily_bar.has_source_revision IS '是否曾至少一次发现同一 (trade_date, code) 的上游数据与本地已有数据不同。true 表示曾发生过源数据修订。';")
    op.execute("COMMENT ON COLUMN market_data.equity_daily_bar.source_revision_count IS '检测到源数据修订的累计次数。只有当同一主键记录的业务字段实际发生变化时才加 1，重复抓取但内容不变不会增加。';")
    op.execute("COMMENT ON COLUMN market_data.equity_daily_bar.last_source_revision_at IS '最近一次检测到源数据修订的本地时间戳。若从未发现修订，则为 NULL。';")
    op.execute("COMMENT ON COLUMN market_data.equity_daily_bar.source_api IS '数据来源 API 路径。当前固定为 /v2/equities/bars/daily。';")
    op.execute("COMMENT ON COLUMN market_data.equity_daily_bar.fetched_at IS '本地抓取并写入数据库的时间戳，不是交易所公告时间。';")
    op.execute("COMMENT ON COLUMN market_data.equity_daily_bar.raw_payload IS 'J-Quants 原始返回 JSON，用于审计、排查和后续补充字段。';")

    op.execute(
        """
        COMMENT ON TABLE market_data.market_calendar IS
        'J-Quants 交易日历表。每行代表一个自然日，用于判断该日是否为交易所营业日。数据来源 /v2/markets/calendar。';
        """
    )
    op.execute(
        """
        COMMENT ON COLUMN market_data.market_calendar.trade_date IS
        '自然日日期，格式为 YYYY-MM-DD。该表以 trade_date 为主键，一天只有一条记录。';
        """
    )
    op.execute(
        """
        COMMENT ON COLUMN market_data.market_calendar.holiday_division IS
        'J-Quants 的 HolDiv 休日区分代码。取值含义为：0 = 非営業日（非营业日，交易所休市）；1 = 営業日（正常营业日）；2 = 東証半日立会日（东京证券交易所半日交易日）；3 = 非営業日(祝日取引あり)（非营业日，但存在祝日交易安排）。';
        """
    )
    op.execute("COMMENT ON COLUMN market_data.market_calendar.source_api IS '数据来源 API 路径。当前固定为 /v2/markets/calendar。';")
    op.execute("COMMENT ON COLUMN market_data.market_calendar.fetched_at IS '本地抓取并写入数据库的时间戳，不是交易所公告时间。';")
    op.execute("COMMENT ON COLUMN market_data.market_calendar.raw_payload IS 'J-Quants 原始返回 JSON，用于审计、排查和后续补充字段。';")

    op.execute(
        """
        DO $$
        BEGIN
            IF to_regclass('public.category_summary') IS NOT NULL THEN
                EXECUTE 'COMMENT ON TABLE public.category_summary IS ''示例统计汇总表。用于保存 demo batch 生成的按 category 聚合结果。''';
                EXECUTE 'COMMENT ON COLUMN public.category_summary.category IS ''分类名称，作为主键。''';
                EXECUTE 'COMMENT ON COLUMN public.category_summary.total IS ''该分类对应的汇总数值。''';
            END IF;
        END
        $$;
        """
    )


def downgrade() -> None:
    for target in [
        "market_data.equity_master_snapshot.raw_payload",
        "market_data.equity_master_snapshot.fetched_at",
        "market_data.equity_master_snapshot.source_api",
        "market_data.equity_master_snapshot.margin_name",
        "market_data.equity_master_snapshot.margin_code",
        "market_data.equity_master_snapshot.market_name",
        "market_data.equity_master_snapshot.market_code",
        "market_data.equity_master_snapshot.scale_category",
        "market_data.equity_master_snapshot.sector33_name",
        "market_data.equity_master_snapshot.sector33_code",
        "market_data.equity_master_snapshot.sector17_name",
        "market_data.equity_master_snapshot.sector17_code",
        "market_data.equity_master_snapshot.company_name_en",
        "market_data.equity_master_snapshot.company_name",
        "market_data.equity_master_snapshot.code",
        "market_data.equity_master_snapshot.as_of_date",
        "market_data.equity_daily_bar.raw_payload",
        "market_data.equity_daily_bar.fetched_at",
        "market_data.equity_daily_bar.source_api",
        "market_data.equity_daily_bar.last_source_revision_at",
        "market_data.equity_daily_bar.source_revision_count",
        "market_data.equity_daily_bar.has_source_revision",
        "market_data.equity_daily_bar.afternoon_adjusted_volume",
        "market_data.equity_daily_bar.afternoon_adjusted_close_price",
        "market_data.equity_daily_bar.afternoon_adjusted_low_price",
        "market_data.equity_daily_bar.afternoon_adjusted_high_price",
        "market_data.equity_daily_bar.afternoon_adjusted_open_price",
        "market_data.equity_daily_bar.afternoon_turnover_value",
        "market_data.equity_daily_bar.afternoon_volume",
        "market_data.equity_daily_bar.afternoon_lower_limit_flag",
        "market_data.equity_daily_bar.afternoon_upper_limit_flag",
        "market_data.equity_daily_bar.afternoon_close_price",
        "market_data.equity_daily_bar.afternoon_low_price",
        "market_data.equity_daily_bar.afternoon_high_price",
        "market_data.equity_daily_bar.afternoon_open_price",
        "market_data.equity_daily_bar.morning_adjusted_volume",
        "market_data.equity_daily_bar.morning_adjusted_close_price",
        "market_data.equity_daily_bar.morning_adjusted_low_price",
        "market_data.equity_daily_bar.morning_adjusted_high_price",
        "market_data.equity_daily_bar.morning_adjusted_open_price",
        "market_data.equity_daily_bar.morning_turnover_value",
        "market_data.equity_daily_bar.morning_volume",
        "market_data.equity_daily_bar.morning_lower_limit_flag",
        "market_data.equity_daily_bar.morning_upper_limit_flag",
        "market_data.equity_daily_bar.morning_close_price",
        "market_data.equity_daily_bar.morning_low_price",
        "market_data.equity_daily_bar.morning_high_price",
        "market_data.equity_daily_bar.morning_open_price",
        "market_data.equity_daily_bar.adjusted_volume",
        "market_data.equity_daily_bar.adjusted_close_price",
        "market_data.equity_daily_bar.adjusted_low_price",
        "market_data.equity_daily_bar.adjusted_high_price",
        "market_data.equity_daily_bar.adjusted_open_price",
        "market_data.equity_daily_bar.adjustment_factor",
        "market_data.equity_daily_bar.turnover_value",
        "market_data.equity_daily_bar.volume",
        "market_data.equity_daily_bar.lower_limit_flag",
        "market_data.equity_daily_bar.upper_limit_flag",
        "market_data.equity_daily_bar.close_price",
        "market_data.equity_daily_bar.low_price",
        "market_data.equity_daily_bar.high_price",
        "market_data.equity_daily_bar.open_price",
        "market_data.equity_daily_bar.code",
        "market_data.equity_daily_bar.trade_date",
        "market_data.market_calendar.raw_payload",
        "market_data.market_calendar.fetched_at",
        "market_data.market_calendar.source_api",
        "market_data.market_calendar.holiday_division",
        "market_data.market_calendar.trade_date",
    ]:
        op.execute(f"COMMENT ON COLUMN {target} IS NULL;")

    op.execute("COMMENT ON TABLE market_data.equity_master_snapshot IS NULL;")
    op.execute("COMMENT ON TABLE market_data.equity_daily_bar IS NULL;")
    op.execute("COMMENT ON TABLE market_data.market_calendar IS NULL;")
    op.execute(
        """
        DO $$
        BEGIN
            IF to_regclass('public.category_summary') IS NOT NULL THEN
                EXECUTE 'COMMENT ON COLUMN public.category_summary.total IS NULL';
                EXECUTE 'COMMENT ON COLUMN public.category_summary.category IS NULL';
                EXECUTE 'COMMENT ON TABLE public.category_summary IS NULL';
            END IF;
        END
        $$;
        """
    )
