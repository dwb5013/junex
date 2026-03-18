from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator


class MetricRecord(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    source: str
    category: str
    value: float = Field(default=0.0)


class EquityMasterRecord(BaseModel):
    model_config = ConfigDict(populate_by_name=True, str_strip_whitespace=True)

    as_of_date: date = Field(alias="Date")
    code: str = Field(alias="Code", min_length=4, max_length=5)
    company_name: str = Field(alias="CoName")
    company_name_en: str | None = Field(default=None, alias="CoNameEn")
    sector17_code: str = Field(alias="S17", min_length=1, max_length=2)
    sector17_name: str = Field(alias="S17Nm")
    sector33_code: str = Field(alias="S33", min_length=1, max_length=4)
    sector33_name: str = Field(alias="S33Nm")
    scale_category: str = Field(alias="ScaleCat")
    market_code: str = Field(alias="Mkt", min_length=4, max_length=4)
    market_name: str = Field(alias="MktNm")
    margin_code: str | None = Field(default=None, alias="Mrgn")
    margin_name: str | None = Field(default=None, alias="MrgnNm")


class EquityDailyBarRecord(BaseModel):
    model_config = ConfigDict(populate_by_name=True, str_strip_whitespace=True)

    trade_date: date = Field(alias="Date")
    code: str = Field(alias="Code", min_length=4, max_length=5)
    open_price: float | None = Field(default=None, alias="O")
    high_price: float | None = Field(default=None, alias="H")
    low_price: float | None = Field(default=None, alias="L")
    close_price: float | None = Field(default=None, alias="C")
    upper_limit_flag: str | None = Field(default=None, alias="UL")
    lower_limit_flag: str | None = Field(default=None, alias="LL")
    volume: float | None = Field(default=None, alias="Vo")
    turnover_value: float | None = Field(default=None, alias="Va")
    adjustment_factor: float | None = Field(default=None, alias="AdjFactor")
    adjusted_open_price: float | None = Field(default=None, alias="AdjO")
    adjusted_high_price: float | None = Field(default=None, alias="AdjH")
    adjusted_low_price: float | None = Field(default=None, alias="AdjL")
    adjusted_close_price: float | None = Field(default=None, alias="AdjC")
    adjusted_volume: float | None = Field(default=None, alias="AdjVo")
    morning_open_price: float | None = Field(default=None, alias="MO")
    morning_high_price: float | None = Field(default=None, alias="MH")
    morning_low_price: float | None = Field(default=None, alias="ML")
    morning_close_price: float | None = Field(default=None, alias="MC")
    morning_upper_limit_flag: str | None = Field(default=None, alias="MUL")
    morning_lower_limit_flag: str | None = Field(default=None, alias="MLL")
    morning_volume: float | None = Field(default=None, alias="MVo")
    morning_turnover_value: float | None = Field(default=None, alias="MVa")
    morning_adjusted_open_price: float | None = Field(default=None, alias="MAdjO")
    morning_adjusted_high_price: float | None = Field(default=None, alias="MAdjH")
    morning_adjusted_low_price: float | None = Field(default=None, alias="MAdjL")
    morning_adjusted_close_price: float | None = Field(default=None, alias="MAdjC")
    morning_adjusted_volume: float | None = Field(default=None, alias="MAdjVo")
    afternoon_open_price: float | None = Field(default=None, alias="AO")
    afternoon_high_price: float | None = Field(default=None, alias="AH")
    afternoon_low_price: float | None = Field(default=None, alias="AL")
    afternoon_close_price: float | None = Field(default=None, alias="AC")
    afternoon_upper_limit_flag: str | None = Field(default=None, alias="AUL")
    afternoon_lower_limit_flag: str | None = Field(default=None, alias="ALL")
    afternoon_volume: float | None = Field(default=None, alias="AVo")
    afternoon_turnover_value: float | None = Field(default=None, alias="AVa")
    afternoon_adjusted_open_price: float | None = Field(default=None, alias="AAdjO")
    afternoon_adjusted_high_price: float | None = Field(default=None, alias="AAdjH")
    afternoon_adjusted_low_price: float | None = Field(default=None, alias="AAdjL")
    afternoon_adjusted_close_price: float | None = Field(default=None, alias="AAdjC")
    afternoon_adjusted_volume: float | None = Field(default=None, alias="AAdjVo")


class TopixDailyBarRecord(BaseModel):
    model_config = ConfigDict(populate_by_name=True, str_strip_whitespace=True)

    trade_date: date = Field(alias="Date")
    open_price: float | None = Field(default=None, alias="O")
    high_price: float | None = Field(default=None, alias="H")
    low_price: float | None = Field(default=None, alias="L")
    close_price: float | None = Field(default=None, alias="C")


class IndexDailyBarRecord(BaseModel):
    model_config = ConfigDict(populate_by_name=True, str_strip_whitespace=True)

    trade_date: date = Field(alias="Date")
    code: str = Field(alias="Code", min_length=4, max_length=4)
    open_price: float | None = Field(default=None, alias="O")
    high_price: float | None = Field(default=None, alias="H")
    low_price: float | None = Field(default=None, alias="L")
    close_price: float | None = Field(default=None, alias="C")


class MarketCalendarRecord(BaseModel):
    model_config = ConfigDict(populate_by_name=True, str_strip_whitespace=True)

    trade_date: date = Field(alias="Date")
    holiday_division: str = Field(alias="HolDiv")


class EarningsCalendarRecord(BaseModel):
    model_config = ConfigDict(populate_by_name=True, str_strip_whitespace=True)

    scheduled_date: date = Field(alias="Date")
    code: str = Field(alias="Code", min_length=4, max_length=5)
    company_name: str = Field(alias="CoName")
    fiscal_year_end: str = Field(alias="FY")
    sector_name: str = Field(alias="SectorNm")
    fiscal_quarter: str = Field(alias="FQ")
    section: str = Field(alias="Section")


class FinsSummaryRecord(BaseModel):
    model_config = ConfigDict(populate_by_name=True, str_strip_whitespace=True)

    disclosure_date: date = Field(alias="DiscDate")
    disclosure_time: str | None = Field(default=None, alias="DiscTime")
    code: str = Field(alias="Code", min_length=4, max_length=5)
    disclosure_number: str = Field(alias="DiscNo")
    document_type: str = Field(alias="DocType")

    current_period_type: str | None = Field(default=None, alias="CurPerType")
    current_period_start: date | None = Field(default=None, alias="CurPerSt")
    current_period_end: date | None = Field(default=None, alias="CurPerEn")
    current_fiscal_year_start: date | None = Field(default=None, alias="CurFYSt")
    current_fiscal_year_end: date | None = Field(default=None, alias="CurFYEn")
    next_fiscal_year_start: date | None = Field(default=None, alias="NxtFYSt")
    next_fiscal_year_end: date | None = Field(default=None, alias="NxtFYEn")

    sales: Decimal | None = Field(default=None, alias="Sales")
    operating_profit: Decimal | None = Field(default=None, alias="OP")
    ordinary_profit: Decimal | None = Field(default=None, alias="OdP")
    net_profit: Decimal | None = Field(default=None, alias="NP")
    eps: Decimal | None = Field(default=None, alias="EPS")
    diluted_eps: Decimal | None = Field(default=None, alias="DEPS")
    total_assets: Decimal | None = Field(default=None, alias="TA")
    equity: Decimal | None = Field(default=None, alias="Eq")
    equity_attributable_ratio: Decimal | None = Field(default=None, alias="EqAR")
    bps: Decimal | None = Field(default=None, alias="BPS")
    cash_flow_from_operating: Decimal | None = Field(default=None, alias="CFO")
    cash_flow_from_investing: Decimal | None = Field(default=None, alias="CFI")
    cash_flow_from_financing: Decimal | None = Field(default=None, alias="CFF")
    cash_and_equivalents: Decimal | None = Field(default=None, alias="CashEq")

    dividend_1q: Decimal | None = Field(default=None, alias="Div1Q")
    dividend_2q: Decimal | None = Field(default=None, alias="Div2Q")
    dividend_3q: Decimal | None = Field(default=None, alias="Div3Q")
    dividend_fy: Decimal | None = Field(default=None, alias="DivFY")
    dividend_annual: Decimal | None = Field(default=None, alias="DivAnn")
    dividend_unit: str | None = Field(default=None, alias="DivUnit")
    dividend_total_annual: Decimal | None = Field(default=None, alias="DivTotalAnn")
    payout_ratio_annual: Decimal | None = Field(default=None, alias="PayoutRatioAnn")

    forecast_dividend_1q: Decimal | None = Field(default=None, alias="FDiv1Q")
    forecast_dividend_2q: Decimal | None = Field(default=None, alias="FDiv2Q")
    forecast_dividend_3q: Decimal | None = Field(default=None, alias="FDiv3Q")
    forecast_dividend_fy: Decimal | None = Field(default=None, alias="FDivFY")
    forecast_dividend_annual: Decimal | None = Field(default=None, alias="FDivAnn")
    forecast_dividend_unit: str | None = Field(default=None, alias="FDivUnit")
    forecast_dividend_total_annual: Decimal | None = Field(default=None, alias="FDivTotalAnn")
    forecast_payout_ratio_annual: Decimal | None = Field(default=None, alias="FPayoutRatioAnn")

    next_forecast_dividend_1q: Decimal | None = Field(default=None, alias="NxFDiv1Q")
    next_forecast_dividend_2q: Decimal | None = Field(default=None, alias="NxFDiv2Q")
    next_forecast_dividend_3q: Decimal | None = Field(default=None, alias="NxFDiv3Q")
    next_forecast_dividend_fy: Decimal | None = Field(default=None, alias="NxFDivFY")
    next_forecast_dividend_annual: Decimal | None = Field(default=None, alias="NxFDivAnn")
    next_forecast_dividend_unit: str | None = Field(default=None, alias="NxFDivUnit")
    next_forecast_payout_ratio_annual: Decimal | None = Field(default=None, alias="NxFPayoutRatioAnn")

    forecast_sales_2q: Decimal | None = Field(default=None, alias="FSales2Q")
    forecast_operating_profit_2q: Decimal | None = Field(default=None, alias="FOP2Q")
    forecast_ordinary_profit_2q: Decimal | None = Field(default=None, alias="FOdP2Q")
    forecast_net_profit_2q: Decimal | None = Field(default=None, alias="FNP2Q")
    forecast_eps_2q: Decimal | None = Field(default=None, alias="FEPS2Q")
    next_forecast_sales_2q: Decimal | None = Field(default=None, alias="NxFSales2Q")
    next_forecast_operating_profit_2q: Decimal | None = Field(default=None, alias="NxFOP2Q")
    next_forecast_ordinary_profit_2q: Decimal | None = Field(default=None, alias="NxFOdP2Q")
    next_forecast_net_profit_2q: Decimal | None = Field(default=None, alias="NxFNp2Q")
    next_forecast_eps_2q: Decimal | None = Field(default=None, alias="NxFEPS2Q")

    forecast_sales_fy: Decimal | None = Field(default=None, alias="FSales")
    forecast_operating_profit_fy: Decimal | None = Field(default=None, alias="FOP")
    forecast_ordinary_profit_fy: Decimal | None = Field(default=None, alias="FOdP")
    forecast_net_profit_fy: Decimal | None = Field(default=None, alias="FNP")
    forecast_eps_fy: Decimal | None = Field(default=None, alias="FEPS")
    next_forecast_sales_fy: Decimal | None = Field(default=None, alias="NxFSales")
    next_forecast_operating_profit_fy: Decimal | None = Field(default=None, alias="NxFOP")
    next_forecast_ordinary_profit_fy: Decimal | None = Field(default=None, alias="NxFOdP")
    next_forecast_net_profit_fy: Decimal | None = Field(default=None, alias="NxFNp")
    next_forecast_eps_fy: Decimal | None = Field(default=None, alias="NxFEPS")

    material_changes_in_subsidiaries: bool | None = Field(default=None, alias="MatChgSub")
    significant_change_in_scope: bool | None = Field(default=None, alias="SigChgInC")
    change_by_accounting_standard_revision: bool | None = Field(default=None, alias="ChgByASRev")
    no_change_by_accounting_standard_revision: bool | None = Field(default=None, alias="ChgNoASRev")
    change_of_accounting_estimates: bool | None = Field(default=None, alias="ChgAcEst")
    retrospective_restatement: bool | None = Field(default=None, alias="RetroRst")

    shares_outstanding_fy: Decimal | None = Field(default=None, alias="ShOutFY")
    treasury_shares_fy: Decimal | None = Field(default=None, alias="TrShFY")
    average_shares: Decimal | None = Field(default=None, alias="AvgSh")

    nc_sales: Decimal | None = Field(default=None, alias="NCSales")
    nc_operating_profit: Decimal | None = Field(default=None, alias="NCOP")
    nc_ordinary_profit: Decimal | None = Field(default=None, alias="NCOdP")
    nc_net_profit: Decimal | None = Field(default=None, alias="NCNP")
    nc_eps: Decimal | None = Field(default=None, alias="NCEPS")
    nc_total_assets: Decimal | None = Field(default=None, alias="NCTA")
    nc_equity: Decimal | None = Field(default=None, alias="NCEq")
    nc_equity_ratio: Decimal | None = Field(default=None, alias="NCEqAR")
    nc_bps: Decimal | None = Field(default=None, alias="NCBPS")

    forecast_nc_sales_2q: Decimal | None = Field(default=None, alias="FNCSales2Q")
    forecast_nc_operating_profit_2q: Decimal | None = Field(default=None, alias="FNCOP2Q")
    forecast_nc_ordinary_profit_2q: Decimal | None = Field(default=None, alias="FNCOdP2Q")
    forecast_nc_net_profit_2q: Decimal | None = Field(default=None, alias="FNCNP2Q")
    forecast_nc_eps_2q: Decimal | None = Field(default=None, alias="FNCEPS2Q")
    next_forecast_nc_sales_2q: Decimal | None = Field(default=None, alias="NxFNCSales2Q")
    next_forecast_nc_operating_profit_2q: Decimal | None = Field(default=None, alias="NxFNCOP2Q")
    next_forecast_nc_ordinary_profit_2q: Decimal | None = Field(default=None, alias="NxFNCOdP2Q")
    next_forecast_nc_net_profit_2q: Decimal | None = Field(default=None, alias="NxFNCNP2Q")
    next_forecast_nc_eps_2q: Decimal | None = Field(default=None, alias="NxFNCEPS2Q")

    forecast_nc_sales_fy: Decimal | None = Field(default=None, alias="FNCSales")
    forecast_nc_operating_profit_fy: Decimal | None = Field(default=None, alias="FNCOP")
    forecast_nc_ordinary_profit_fy: Decimal | None = Field(default=None, alias="FNCOdP")
    forecast_nc_net_profit_fy: Decimal | None = Field(default=None, alias="FNCNP")
    forecast_nc_eps_fy: Decimal | None = Field(default=None, alias="FNCEPS")
    next_forecast_nc_sales_fy: Decimal | None = Field(default=None, alias="NxFNCSales")
    next_forecast_nc_operating_profit_fy: Decimal | None = Field(default=None, alias="NxFNCOP")
    next_forecast_nc_ordinary_profit_fy: Decimal | None = Field(default=None, alias="NxFNCOdP")
    next_forecast_nc_net_profit_fy: Decimal | None = Field(default=None, alias="NxFNCNP")
    next_forecast_nc_eps_fy: Decimal | None = Field(default=None, alias="NxFNCEPS")

    @model_validator(mode="before")
    @classmethod
    def _empty_strings_to_none(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        return {key: (None if value == "" else value) for key, value in data.items()}


class FinsDividendRecord(BaseModel):
    model_config = ConfigDict(populate_by_name=True, str_strip_whitespace=True)

    publication_date: date = Field(alias="PubDate")
    publication_time: str = Field(alias="PubTime")
    code: str = Field(alias="Code", min_length=4, max_length=5)
    reference_number: str = Field(alias="RefNo")
    status_code: str = Field(alias="StatCode")
    board_meeting_date: date = Field(alias="BoardDate")
    interim_final_code: str = Field(alias="IFCode")
    forecast_revision_code: str = Field(alias="FRCode")
    interim_final_term: str = Field(alias="IFTerm")
    dividend_rate: str | None = Field(default=None, alias="DivRate")
    record_date: date = Field(alias="RecDate")
    ex_rights_date: date = Field(alias="ExDate")
    actual_record_date: date = Field(alias="ActRecDate")
    payment_start_date: str | None = Field(default=None, alias="PayDate")
    corporate_action_reference_number: str = Field(alias="CARefNo")
    distribution_amount: str | None = Field(default=None, alias="DistAmt")
    retained_earnings_amount: str | None = Field(default=None, alias="RetEarn")
    deemed_dividend_amount: str | None = Field(default=None, alias="DeemDiv")
    deemed_capital_gains_amount: str | None = Field(default=None, alias="DeemCapGains")
    net_asset_decrease_ratio: str | None = Field(default=None, alias="NetAssetDecRatio")
    commemorative_special_code: str = Field(alias="CommSpecCode")
    commemorative_dividend_rate: str | None = Field(default=None, alias="CommDivRate")
    special_dividend_rate: str | None = Field(default=None, alias="SpecDivRate")

    @model_validator(mode="before")
    @classmethod
    def _normalize_optional_strings(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        text_like_fields = {
            "DivRate",
            "PayDate",
            "DistAmt",
            "RetEarn",
            "DeemDiv",
            "DeemCapGains",
            "NetAssetDecRatio",
            "CommDivRate",
            "SpecDivRate",
        }
        normalized: dict[str, Any] = {}
        for key, value in data.items():
            if value == "":
                normalized[key] = None
            elif key in text_like_fields and value is not None:
                normalized[key] = str(value)
            else:
                normalized[key] = value
        return normalized
