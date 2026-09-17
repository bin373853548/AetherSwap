import copy
from typing import Any, Optional
DEFAULTS = {
    "iflow": {
        "page_num": 1,
        "page_size": 200,
        "platforms": "buff",
        "sort_by": "sell",
        "min_price": 2,
        "max_price": 5000,
        "min_volume": 200,
        "type": "swap",
        "want_to_get": "STEAM_BALANCE",
        "sale_plan": "STEAM_SELL_PRICE",
        "fetch_timeout": 15,
    },
    "buff": {
        "pay_method": "alipay",
        "game": "csgo",
        "price_tolerance": 0.5,
    },
    "stability": {
        "days": 30,
        "cv_threshold": 0.05,
        "r2_threshold": 0.6,
        "min_daily_trades": 5,
        "price_percentile_ceil": 0.8,
        "r2_rising_threshold": 0.8,
        "slope_pct_ceil": 0.01,
        "ma_deviation_ceil": 1.1,
        "last_price_ma30_ceil": 1.05,
        "slope_stable_floor": -0.005,
        "price_percentile_ceil_rising": 0.5,
        "use_vwap": True,
        "request_interval_seconds": 2.5,
        "request_failure_delay_seconds": 5,
    },
    "pipeline": {
        "target_balance": 100,
        "max_discount": 0.9,
        "huge_profit_offset": 0.05,
        "iflow_top_n": 50,
        "exclude_keywords": ["印花"],
        "verbose_debug": False,
        "sell_strategy": 4,
        "sell_price_offset": 0,
        "sell_price_wall_volume": 20,
        "sell_price_max_ignore_volume": 4,
        "sell_price_min_tier_volume": 3,
        "sell_price_max_ratio": 1.1,
        "sell_anchor_max_ratio": 1.05,
        "sell_anchor_days": 3,
        "sell_liquidity_ratio": 0.05,
        "sell_trend_days": 7,
        "retry_interval_seconds": 300,
        "buff_retry_delay_seconds": 5,
        "current_price_refresh_minutes": 10,
        "resell_ratio": 0.85,
        "safe_purchase_hard_qty_cap": 50,
        "safe_purchase_liquidity_ratio": 0.05,
        "safe_purchase_low_price_threshold": 5.0,
        "safe_purchase_low_price_penalty": 0.5,
        "safe_purchase_low_price_hard_cap": 30,
        "sell_pressure_orders_n": 5,
        "sell_pressure_threshold": 2.0,
        "receive_poll_interval_seconds": 30,
        "listing_check_interval_seconds": 600,
        "max_listings_per_item": 5,
        "listing_delay_seconds": 3,
        "sell_reconcile_enabled": True,
        "sell_cost_floor_enabled": True,
        "sell_cost_floor_ratio": 1.0,
        "sell_reprice_enabled": True,
        "sell_reprice_min_drop_pct": 5,
        "sell_reprice_max_age_hours": 72,
        "sell_reprice_min_interval_hours": 6,
        "steam_listings_debug": False,
        "start_time_limit_enabled": False,
        "start_time_hour": 8,
        "end_time_hour": 22,
    },
    "inventory": {
        "refresh_seconds": 600,
    },
    "notify": {
        "pushplus_token": "",
        "holdings_report_interval_hours": 0,
        "holdings_report_change_threshold_pct": 20,
        "holdings_report_drop_enabled": True,
        "email_user": "",
        "email_pass": "",
        "imap_server": "imap.qq.com",
        "target_sender": "",
        "subject_success": "已确认成功付款",
        "subject_fail": "已确认付款失败",
        "allowed_sender": "",
        "email_timeout_seconds": 300,
    },
    "steam_guard": {
        "shared_secret": "",
    },
    "steam_confirm": {
        "enabled": False,
        "identity_secret": "",
        "device_id": "",
    },
    "system": {
        "exchange_rate_refresh_hours": 24,
        "buff_session_keepalive_enabled": False,
        "session_keepalive_hours": 4.0,
        # The web UI records the browser clock zone here.  Keeping both an
        # IANA name and an offset lets containers/Windows hosts without a
        # matching zone database still evaluate schedule windows correctly.
        "timezone": "Asia/Shanghai",
        "timezone_offset_minutes": 480,
        "ui_scale": "0.7",
    },
    "proxy_pool": {
        "enabled": False,
        "strategy": 3,
        "test_url": "https://ipv4.webshare.io/",
        "timeout_seconds": 10,
        "webshare_api_key": "",
        "proxies": [],
    },
    "steam_deals": {
        "enabled": False,
        "auto_refresh_days": 7,
        "max_game_threads": 5,
        "max_region_threads": 16,
    },
    "strategies": {
        "active_buy_strategy_id": "system.buy.default",
        "active_sell_strategy_id": "",
    },
}
def merge(default: dict, overrides: dict) -> dict:
    out = copy.deepcopy(default)
    for k, v in overrides.items():
        if k in out and isinstance(out[k], dict) and isinstance(v, dict):
            out[k] = merge(out[k], v)
        else:
            out[k] = copy.deepcopy(v)
    return out


def _coerce_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"1", "true", "yes", "y", "on"}:
            return True
        if text in {"0", "false", "no", "n", "off"}:
            return False
        return default
    if isinstance(value, (int, float)):
        return bool(value)
    return default
def _validate_ranges(cfg: dict) -> dict:
    # 简单校验一下，防止用户乱填配置搞崩程序
    import warnings
    pipe = cfg.get("pipeline") or {}
    stab = cfg.get("stability") or {}
    buff = cfg.get("buff") or {}
    inv = cfg.get("inventory") or {}
    system = cfg.get("system") or {}

    if isinstance(pipe.get("max_discount"), (int, float)):
        v = pipe["max_discount"]
        if not (0 < v <= 1):
            warnings.warn(f"[config] pipeline.max_discount={v} 超出范围(0,1]，已修正为 {min(max(v, 0.001), 1.0):.4g}")
            pipe["max_discount"] = min(max(v, 0.001), 1.0)

    if isinstance(stab.get("cv_threshold"), (int, float)):
        v = stab["cv_threshold"]
        if not (0 < v < 1):
            warnings.warn(f"[config] stability.cv_threshold={v} 超出范围(0,1)，已修正")
            stab["cv_threshold"] = max(0.001, min(v, 0.999))

    if isinstance(stab.get("r2_threshold"), (int, float)):
        v = stab["r2_threshold"]
        if not (0 < v < 1):
            warnings.warn(f"[config] stability.r2_threshold={v} 超出范围(0,1)，已修正")
            stab["r2_threshold"] = max(0.001, min(v, 0.999))

    if isinstance(stab.get("price_percentile_ceil"), (int, float)):
        v = stab["price_percentile_ceil"]
        if not (0 < v <= 1):
            warnings.warn(f"[config] stability.price_percentile_ceil={v} 超出范围(0,1]，已修正")
            stab["price_percentile_ceil"] = max(0.001, min(v, 1.0))

    # price_percentile_ceil_rising 同上
    if isinstance(stab.get("price_percentile_ceil_rising"), (int, float)):
        v = stab["price_percentile_ceil_rising"]
        if not (0 < v <= 1):
            stab["price_percentile_ceil_rising"] = max(0.001, min(v, 1.0))

    if isinstance(pipe.get("sell_price_max_ratio"), (int, float)):
        v = pipe["sell_price_max_ratio"]
        if not (0 < v <= 10):
            warnings.warn(f"[config] pipeline.sell_price_max_ratio={v} 超出范围(0,10]，已修正为 {min(max(v, 0.01), 10.0):.4g}")
            pipe["sell_price_max_ratio"] = min(max(v, 0.01), 10.0)

    if isinstance(pipe.get("sell_price_min_tier_volume"), (int, float)):
        v = pipe["sell_price_min_tier_volume"]
        if v < 0:
            warnings.warn(f"[config] pipeline.sell_price_min_tier_volume={v} 不能为负数，已修正为0")
            pipe["sell_price_min_tier_volume"] = 0

    if isinstance(pipe.get("sell_anchor_max_ratio"), (int, float)):
        v = pipe["sell_anchor_max_ratio"]
        if not (0 <= v <= 5):
            warnings.warn(f"[config] pipeline.sell_anchor_max_ratio={v} 超出范围[0,5]，已修正为 {min(max(v, 0.0), 5.0):.4g}")
            pipe["sell_anchor_max_ratio"] = min(max(v, 0.0), 5.0)

    if isinstance(pipe.get("sell_anchor_days"), (int, float)):
        v = pipe["sell_anchor_days"]
        if v < 1:
            warnings.warn(f"[config] pipeline.sell_anchor_days={v} 至少为1，已修正为1")
            pipe["sell_anchor_days"] = 1

    if isinstance(pipe.get("sell_liquidity_ratio"), (int, float)):
        v = pipe["sell_liquidity_ratio"]
        if v < 0:
            warnings.warn(f"[config] pipeline.sell_liquidity_ratio={v} 不能为负数，已修正为0")
            pipe["sell_liquidity_ratio"] = 0.0

    if isinstance(pipe.get("sell_cost_floor_ratio"), (int, float)):
        v = pipe["sell_cost_floor_ratio"]
        if v < 0:
            warnings.warn(f"[config] pipeline.sell_cost_floor_ratio={v} 不能为负数，已修正为0")
            pipe["sell_cost_floor_ratio"] = 0.0

    if isinstance(pipe.get("sell_reprice_min_drop_pct"), (int, float)):
        v = pipe["sell_reprice_min_drop_pct"]
        if v < 0:
            warnings.warn(f"[config] pipeline.sell_reprice_min_drop_pct={v} 不能为负数，已修正为0")
            pipe["sell_reprice_min_drop_pct"] = 0.0

    if isinstance(pipe.get("sell_reprice_max_age_hours"), (int, float)):
        v = pipe["sell_reprice_max_age_hours"]
        if v < 0:
            warnings.warn(f"[config] pipeline.sell_reprice_max_age_hours={v} 不能为负数，已修正为0")
            pipe["sell_reprice_max_age_hours"] = 0.0

    if isinstance(pipe.get("sell_reprice_min_interval_hours"), (int, float)):
        v = pipe["sell_reprice_min_interval_hours"]
        if v < 0:
            warnings.warn(f"[config] pipeline.sell_reprice_min_interval_hours={v} 不能为负数，已修正为0")
            pipe["sell_reprice_min_interval_hours"] = 0.0

    if isinstance(buff.get("price_tolerance"), (int, float)):
        v = buff["price_tolerance"]
        if v < 0:
            warnings.warn(f"[config] buff.price_tolerance={v} 不能为负数，已修正为0")
            buff["price_tolerance"] = 0.0

    if isinstance(inv.get("refresh_seconds"), (int, float)):
        v = inv["refresh_seconds"]
        if 0 < v < 600:
            warnings.warn(f"[config] inventory.refresh_seconds={v} 过短，已修正为600秒")
            inv["refresh_seconds"] = 600

    timezone_name = system.get("timezone")
    system["timezone"] = (
        timezone_name.strip()[:128]
        if isinstance(timezone_name, str)
        else ""
    )
    offset = system.get("timezone_offset_minutes")
    if isinstance(offset, bool):
        offset = None
    if offset is not None:
        try:
            parsed_offset = float(offset)
            if not parsed_offset.is_integer() or not -840 <= parsed_offset <= 840:
                raise ValueError("timezone offset out of range")
            system["timezone_offset_minutes"] = int(parsed_offset)
        except (TypeError, ValueError, OverflowError):
            system["timezone_offset_minutes"] = None

    return cfg


def get_app_config(loaded: dict) -> dict:
    return _validate_ranges(merge(DEFAULTS, loaded.get("app", {})))
def validate_and_fill(data: dict, defaults: Optional[dict] = None) -> dict:
    if defaults is None:
        defaults = DEFAULTS
    out = {}
    for k, default in defaults.items():
        if k not in data:
            out[k] = dict(default) if isinstance(default, dict) else default
        elif isinstance(default, dict) and isinstance(data[k], dict):
            out[k] = validate_and_fill(merge(default, data[k]), default)
        else:
            val = data[k]
            if isinstance(default, bool):
                if not isinstance(val, bool):
                    val = _coerce_bool(val, default)
            elif isinstance(default, int):
                if isinstance(val, bool):
                    val = default
                elif not isinstance(val, int):
                    try:
                        val = int(float(val))
                    except (ValueError, TypeError, OverflowError):
                        val = default
            elif isinstance(default, float):
                if isinstance(val, bool):
                    val = default
                elif not isinstance(val, float):
                    try:
                        val = float(val)
                    except (ValueError, TypeError, OverflowError):
                        val = default
            elif isinstance(default, str) and not isinstance(val, str):
                val = default
            elif isinstance(default, list) and not isinstance(val, list):
                val = default
            out[k] = val
    return out
