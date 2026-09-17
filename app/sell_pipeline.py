import json
import re
import statistics
import threading
import time
from collections import defaultdict
from datetime import timedelta
from pathlib import Path
from typing import Optional

from app.accounts import get_current_account
from app.config_loader import get_steam_credentials, load_app_config_validated
from app.config_schema import DEFAULTS, merge
from app.inventory_cs2 import scan_cs2_inventory
from app.pipeline_context import PipelineContext
from app.services.account_region import refresh_account_region_currency
from app.strategy_engine import apply_strategy_to_config, evaluate_strategy_runtime_modules
from app.state import get_state, append_sale
from app.steam_confirm import auto_confirm_once
from app.steam_delist import delist_item
from app.steam_listings import fetch_my_listings
from steam.market import list_item, parse_sell_response
from steam.market_orders import compute_smart_list_price, get_sell_orders_cny
from steam.request_policy import MarketCooldown
from steam.session import create_market_session
from utils.delay import jittered_sleep
from utils.money import STEAM_FEE_FACTOR, USD_TO_CNY_DEFAULT, list_price_display_to_cents
from utils.time import parse_steam_history_date
from utils.trend import calculate_trend_robust

_sell_phase_lock = threading.Lock()
_listing_cooldown = MarketCooldown()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_EMPTY_HISTORY_SIGNALS = {
    "latest_price": None,
    "trend": None,
    "median_price": None,
    "anchor_price": None,
    "daily_volume": 0.0,
    "sample_size": 0,
}
_ANCHOR_MIN_TRADES = 3


def _steam_history_signals(market_hash_name: str, window_days: int = 7, anchor_days: int = 3) -> dict:
    """Summarise Steam sale history for pricing.

    Returns ``latest_price``/``trend``, the ``median_price`` of the most recent
    *anchor_days* of trades, the ``anchor_price`` used as the pricing ceiling
    (the higher of that median and the latest trade, so a rally is not capped by
    a stale median), and the average ``daily_volume`` over *window_days*.
    Everything degrades to None/0 when the history is unavailable so callers
    never have to special-case a failed fetch.
    """
    from app.services.steam_client import SteamClient
    from utils.money import apply_currency
    client = SteamClient()
    raw = client.fetch_history(market_hash_name, app_id=730, return_currency=True)
    if not raw or not isinstance(raw, dict):
        return dict(_EMPTY_HISTORY_SIGNALS)
    history = raw.get("history")
    currency = raw.get("currency")
    if not history:
        return dict(_EMPTY_HISTORY_SIGNALS)
    parsed = []
    for entry in history:
        if len(entry) < 2:
            continue
        dt = parse_steam_history_date(str(entry[0]))
        if dt is None:
            continue
        try:
            price = float(entry[1])
        except (ValueError, TypeError):
            continue
        try:
            volume = float(entry[2]) if len(entry) > 2 and entry[2] is not None else 0.0
        except (ValueError, TypeError):
            volume = 0.0
        parsed.append((dt, price, volume))
    if not parsed:
        return dict(_EMPTY_HISTORY_SIGNALS)
    prices_cny, _ = apply_currency([x[1] for x in parsed], currency, USD_TO_CNY_DEFAULT)
    if not prices_cny:
        return dict(_EMPTY_HISTORY_SIGNALS)
    rows = sorted(
        zip([x[0] for x in parsed], prices_cny, [x[2] for x in parsed]),
        key=lambda x: x[0],
    )
    newest_dt = rows[-1][0]
    window_days = max(1, int(window_days))
    window_cutoff = newest_dt - timedelta(days=window_days)
    window_rows = [r for r in rows if r[0] >= window_cutoff]
    window_prices = [r[1] for r in window_rows]
    trend = (
        calculate_trend_robust(window_prices, use_dynamic_sensitivity=True)
        if len(window_prices) >= 3
        else 0
    )
    total_volume = sum(r[2] for r in window_rows)
    anchor_cutoff = newest_dt - timedelta(days=max(1, int(anchor_days)))
    anchor_prices = [r[1] for r in rows if r[0] >= anchor_cutoff]
    latest_price = rows[-1][1]
    median_price = statistics.median(anchor_prices) if len(anchor_prices) >= _ANCHOR_MIN_TRADES else None
    return {
        "latest_price": latest_price,
        "trend": trend,
        "median_price": median_price,
        "anchor_price": max(median_price, latest_price) if median_price is not None else None,
        "daily_volume": total_volume / window_days,
        "sample_size": len(window_prices),
    }


def _load_rate_map() -> dict:
    """Load exchange rate JSON from config dir. Returns an empty dict on any failure."""
    try:
        fx_file = Path(__file__).resolve().parent.parent / "config" / "exchange_rate.json"
        if fx_file.exists():
            with open(fx_file, "r", encoding="utf-8") as f:
                fx = json.load(f)
            if isinstance(fx, dict) and isinstance(fx.get("rates"), dict):
                return {k: float(v) for k, v in fx["rates"].items() if isinstance(v, (int, float))}
    except Exception:
        pass
    return {}


def _record_listing_success(
    ctx,
    aid: str,
    name: str,
    list_price: float,
    listing_delay: float,
    kind: str = "list",
) -> None:
    """Append sale record, mark purchase as listed, and sleep the listing delay."""
    append_sale({
        "name": name, "goods_id": 0, "price": list_price, "at": time.time(),
        "assetid": aid or "", "kind": kind,
    })
    if aid:
        purchases = ctx.state.get_purchases()
        for i, p in enumerate(purchases):
            if str(p.get("assetid") or "") == aid:
                db_id = p.get("_db_id")
                if db_id:
                    ctx.state.update_purchase_by_id(db_id, {"listing": True})
                else:
                    ctx.state.update_purchase(i, {"listing": True})
                break
    jittered_sleep(listing_delay)



def _history_signals_cached(cache: dict, market_hash_name: str, window_days: int, anchor_days: int) -> dict:
    """Return :func:`_steam_history_signals` for *market_hash_name*, memoised per round."""
    if market_hash_name not in cache:
        cache[market_hash_name] = _steam_history_signals(
            market_hash_name, window_days=window_days, anchor_days=anchor_days
        )
    return cache[market_hash_name]


def _cost_floor_breach(cost, list_price, floor_ratio):
    """Return ``(net_proceeds, required_price)`` when *list_price* undercuts the cost floor.

    ``net_proceeds`` approximates the seller's proceeds after the Steam fee and
    ``required_price`` is the buyer-pays price that would just reach
    ``cost × floor_ratio``.  ``None`` means the listing is acceptable.
    """
    try:
        cost = float(cost or 0)
        list_price = float(list_price or 0)
        ratio = float(floor_ratio if floor_ratio is not None else 1.0)
    except (TypeError, ValueError):
        return None
    if cost <= 0 or list_price <= 0 or ratio <= 0:
        return None
    net_proceeds = list_price / STEAM_FEE_FACTOR
    if net_proceeds >= cost * ratio:
        return None
    return net_proceeds, round(cost * ratio * STEAM_FEE_FACTOR, 2)


def _pricing_params(pipeline_cfg: dict) -> dict:
    """Read the shared Steam smart-pricing parameters from pipeline config."""
    return {
        "wall_volume": int(pipeline_cfg.get("sell_price_wall_volume", 20)),
        "max_ignore": int(pipeline_cfg.get("sell_price_max_ignore_volume", 4)),
        "min_tier_volume": max(0, int(pipeline_cfg.get("sell_price_min_tier_volume", 3) or 0)),
        "max_price_ratio": pipeline_cfg.get("sell_price_max_ratio"),
        "sell_offset": float(pipeline_cfg.get("sell_price_offset", 0)),
        "anchor_max_ratio": pipeline_cfg.get("sell_anchor_max_ratio"),
        "anchor_days": int(pipeline_cfg.get("sell_anchor_days", 3) or 3),
        "liquidity_ratio": float(pipeline_cfg.get("sell_liquidity_ratio", 0) or 0),
    }


def _fetch_steam_target_price(
    session,
    market_hash_name: str,
    appid: int,
    params: dict,
    recent_sale_price: Optional[float] = None,
    anchor_max_ratio: Optional[float] = None,
):
    """Fetch Steam sell orders and compute the target listing price (CNY).

    Returns ``(list_price, reason, orders_data, error)``.  ``orders_data`` is
    ``None`` when the order book could not be read and ``error`` then explains
    why.
    """
    try:
        orders_result = get_sell_orders_cny(
            session,
            market_hash_name,
            app_id=appid,
            return_error=True,
        )
        if isinstance(orders_result, tuple) and len(orders_result) == 2:
            orders_data, orders_error = orders_result
        else:
            orders_data, orders_error = orders_result, None
    except Exception as e:
        return None, "", None, f"拉取卖单异常: {type(e).__name__} - {e}"
    if not orders_data or not orders_data.get("sell_orders"):
        return None, "", None, f"无法获取 Steam 卖单：{orders_error or '未知原因'}"
    list_price, reason = compute_smart_list_price(
        orders_data["sell_orders"],
        wall_volume_threshold=params["wall_volume"],
        max_ignore_volume=params["max_ignore"],
        min_lowest_tier_volume=params["min_tier_volume"],
        max_price_ratio=params["max_price_ratio"],
        offset=params["sell_offset"],
        recent_sale_price=recent_sale_price,
        anchor_max_ratio=anchor_max_ratio,
    )
    return list_price, reason, orders_data, None


def _latest_listing_records(sales_snapshot: list) -> dict:
    """Map each asset id to its newest listing record.

    ``_record_listing_success`` appends one row per successful listing, so the
    newest row carries the price (CNY) and time that item currently sits at.
    """
    latest: dict = {}
    for row in sales_snapshot or []:
        aid = str(row.get("assetid") or "").strip()
        if not aid:
            continue
        prev = latest.get(aid)
        if prev is None or float(row.get("at") or 0) > float(prev.get("at") or 0):
            latest[aid] = row
    return latest


def _should_reprice(
    current_price,
    target_price,
    age_hours,
    min_drop_pct: float,
    max_age_hours: float,
    min_interval_hours: float,
    record_kind: Optional[str] = None,
):
    """Decide whether a live listing should be delisted and relisted.

    Returns a human-readable reason, or ``None`` to keep the listing as is.
    """
    try:
        current_price = float(current_price)
        target_price = float(target_price)
    except (TypeError, ValueError):
        return None
    if current_price <= 0 or target_price <= 0:
        return None
    if (
        min_interval_hours > 0
        and record_kind == "reprice"
        and age_hours is not None
        and age_hours < min_interval_hours
    ):
        return None
    drop_pct = (current_price - target_price) / current_price * 100
    if drop_pct >= min_drop_pct:
        return f"新价低于现价 {drop_pct:.1f}%"
    if age_hours is not None and age_hours >= max_age_hours:
        change_pct = abs(target_price - current_price) / current_price * 100
        if change_pct >= 1.0:
            direction = "低于" if target_price < current_price else "高于"
            return f"挂单已 {age_hours:.0f} 小时且新价{direction}现价 {change_pct:.1f}%"
    return None


def _update_purchase_for_assetid(ctx: PipelineContext, aid: str, data: dict) -> bool:
    """Apply *data* to the purchase record owning *aid*; returns whether it exists."""
    aid = str(aid or "").strip()
    if not aid:
        return False
    for i, p in enumerate(ctx.state.get_purchases()):
        if str(p.get("assetid") or "").strip() != aid:
            continue
        db_id = p.get("_db_id")
        if db_id:
            ctx.state.update_purchase_by_id(db_id, data)
        else:
            ctx.state.update_purchase(i, data)
        return True
    return False


def _rebind_purchase_assetid(ctx: PipelineContext, old_aid: str, new_aid: Optional[str]) -> None:
    """Re-point the purchase record from *old_aid* to *new_aid* and clear listing flags.

    ``new_aid`` may be ``None`` when Steam rotated the asset id but the new one
    could not be read; the stored asset id is then cleared so the next
    inventory/sold sync can refill it by name instead of keeping a dead id.
    """
    _update_purchase_for_assetid(ctx, old_aid, {
        "assetid": new_aid,
        "listing": False,
        "listing_status": None,
    })


# ---------------------------------------------------------------------------
# Phase sub-functions
# ---------------------------------------------------------------------------

def _resolve_steam_session(ctx: PipelineContext, cred_steam: dict):
    """Validate credentials and create a Steam market session.

    Returns ``(session, session_id_effective)`` or ``None`` if setup fails.
    """
    steam_id = cred_steam.get("steam_id")
    session_id = cred_steam.get("session_id")
    cookies = cred_steam.get("cookies")
    if not steam_id or not session_id or not cookies:
        ctx.log("未配置 Steam steam_id / session_id / cookies，跳过出售阶段", "warn", category="steam")
        return None
    session = create_market_session(cookies, steam_id)
    session_id_effective = session.cookies.get("sessionid") or session_id
    if not session_id_effective:
        ctx.log("Cookie 中无 sessionid，无法上架", "warn", category="steam")
        return None
    return session, session_id_effective


def _get_inventory(ctx: PipelineContext, items: Optional[list]) -> Optional[list]:
    """Return the inventory item list, scanning from Steam if *items* is ``None``.

    Attempts auto-relogin once on auth expiry.  Returns ``None`` when the
    inventory cannot be obtained and the sell phase should abort.
    """
    if items is not None:
        return items
    ok, items, err = scan_cs2_inventory()
    if not ok and err and ("登录已过期" in err or "重新登录" in err):
        ctx.log("Steam 登录已过期，尝试自动重新登录…", "warn", category="steam")
        try:
            from app.services.steam_auth import try_steam_auto_relogin
            relogin_ok, _, relogin_msg = try_steam_auto_relogin()
            if relogin_ok:
                ctx.log(f"自动重新登录成功: {relogin_msg}，重新获取库存", "info", category="steam")
                jittered_sleep(2, jitter_ratio=0.2)
                ok, items, err = scan_cs2_inventory()
            else:
                ctx.log(f"自动重新登录失败: {relogin_msg}", "warn", category="steam")
        except Exception as e:
            ctx.log(f"自动重新登录异常: {type(e).__name__} - {e}", "error", category="steam")
    if not ok:
        ctx.log(f"获取库存失败: {err}，跳过", "warn", category="steam")
        return None
    return items


def _build_listing_plan(
    ctx: PipelineContext,
    cfg: dict,
    session,
    sellable: list,
    sell_strategy: int,
    pipeline_cfg: dict,
    purchases_snapshot: list,
    ok_listings: bool,
    active_listing_ids: set,
    listing_assetid_to_name: dict,
    assetid_to_name_map: dict,
    account_currency: str,
    rate_map: dict,
) -> list:
    """Decide which sellable items to actually list and at what price.

    Returns a list of dicts ready for ``_submit_listings``.
    """
    params = _pricing_params(pipeline_cfg)
    min_tier_volume = params["min_tier_volume"]
    max_price_ratio = params["max_price_ratio"]
    sell_offset = params["sell_offset"]
    max_per_item = max(1, int(pipeline_cfg.get("max_listings_per_item", 5) or 5))
    trend_days = int(pipeline_cfg.get("sell_trend_days", 7))
    cost_floor_enabled = bool(pipeline_cfg.get("sell_cost_floor_enabled", True))
    try:
        cost_floor_ratio = float(pipeline_cfg.get("sell_cost_floor_ratio", 1.0))
    except (TypeError, ValueError):
        cost_floor_ratio = 1.0
    anchor_max_ratio = params["anchor_max_ratio"]
    anchor_days = params["anchor_days"]
    liquidity_ratio = params["liquidity_ratio"]
    try:
        anchor_enabled = float(anchor_max_ratio) > 0
    except (TypeError, ValueError):
        anchor_enabled = False
    need_history = bool(anchor_enabled or liquidity_ratio > 0 or sell_strategy in (2, 3))

    to_list_by_name: dict = defaultdict(int)
    skip_same_name_cap: dict = defaultdict(int)
    skip_liquidity_cap: dict = defaultdict(int)
    to_list = []
    seen_assetids: set = set()
    history_cache: dict = {}

    for it in sellable:
        if ctx.is_stop_requested():
            ctx.set_status("stopped", "已停止")
            return to_list

        name = it.get("name") or ""
        market_hash_name = (it.get("market_hash_name") or name).strip()
        aid = str(it.get("assetid", "")).strip()

        if aid in seen_assetids:
            continue
        seen_assetids.add(aid)
        if not market_hash_name:
            ctx.log(f"[出售] 跳过无名物品 assetid={aid}", "info", category="steam")
            continue
        if ok_listings and aid in active_listing_ids:
            ctx.log(
                f"[出售] {name} assetid={aid} 已在 Steam 在售列表中，跳过重复上架",
                "info", category="steam",
            )
            continue
        
        buy_record = _find_buy_record(purchases_snapshot, aid, market_hash_name)
        if not buy_record:
            ctx.log(f"[出售] {name} assetid={aid} 不在本地购买记录中（非倒余额库>存），为保护个人物品跳过出售", "info", category="steam")
            continue

        # Same-name in-steam cap check
        if ok_listings and listing_assetid_to_name:
            steam_same_name = sum(
                1 for lid in active_listing_ids
                if (listing_assetid_to_name.get(lid) or "").strip() == market_hash_name
            )
        elif ok_listings:
            steam_same_name = sum(
                1 for lid in active_listing_ids
                if (assetid_to_name_map.get(lid) or "").strip() == market_hash_name
            )
        else:
            steam_same_name = sum(
                1 for p in purchases_snapshot
                if p.get("listing") and ((p.get("market_hash_name") or p.get("name") or "").strip() == market_hash_name)
            )

        already_in_this_round = to_list_by_name.get(market_hash_name, 0)
        if steam_same_name + already_in_this_round >= max_per_item:
            skip_same_name_cap[market_hash_name] += 1
            continue

        signals = (
            _history_signals_cached(history_cache, market_hash_name, trend_days, anchor_days)
            if need_history
            else dict(_EMPTY_HISTORY_SIGNALS)
        )
        liquidity_output = {}
        if liquidity_ratio > 0 and signals["daily_volume"] > 0:
            liquidity_cap = max(1, int(signals["daily_volume"] * liquidity_ratio))
            if steam_same_name + already_in_this_round >= liquidity_cap:
                skip_liquidity_cap[market_hash_name] += 1
                ctx.log(
                    f"[出售] {name} assetid={aid} 卖出侧流动性限制：日成交约 {signals['daily_volume']:.1f} 件 × "
                    f"{liquidity_ratio:g} 最多在售 {liquidity_cap} 件（已 {steam_same_name}），跳过",
                    "info", category="steam",
                )
                continue
            liquidity_output = {
                "daily_volume": round(signals["daily_volume"], 2),
                "ratio": liquidity_ratio,
                "cap": liquidity_cap,
            }

        list_price, reason, orders_data, orders_error = _fetch_steam_target_price(
            session, market_hash_name, int(it.get("appid", 730)), params,
            recent_sale_price=signals["anchor_price"],
            anchor_max_ratio=anchor_max_ratio,
        )
        if orders_data is None:
            level = "error" if "异常" in (orders_error or "") else "warn"
            ctx.log(f"[出售] {name} assetid={aid} {orders_error}，跳过", level, category="steam")
            continue
        if list_price is None or list_price <= 0:
            ctx.log(f"[出售] {name} assetid={aid} 无法计算定价({reason})，跳过", "warn", category="steam")
            continue
        list_price = round(float(list_price), 2)
        trend = signals["trend"] if need_history else None
        profit_output = {}

        # Currency conversion for display
        display_price = list_price
        if account_currency != "CNY":
            rate = rate_map.get(account_currency)
            if not rate:
                # 汇率缺失时直接终止整批上架，避免以CNY数值直接提交非人民币账号造成大额亏损
                ctx.log(
                    f"[出售] 账号币种={account_currency}，但汇率文件缺少该币种数据，"
                    "无法安全定价，终止本次出售以防价格错误（可等汇率更新后重试）",
                    "error", category="steam",
                )
                return []
            display_price = round(list_price / rate, 2)
            ctx.debug(
                f"[出售] 账号币种={account_currency}, CNY={list_price:.2f} -> {account_currency}={display_price:.2f}",
                category="steam",
            )

        # Sell strategy 2/3: skip if rising trend
        if sell_strategy in (2, 3) and trend is not None and trend > 0:
            ctx.log(f"[出售] {name} assetid={aid} 近{trend_days}天上升趋势，等待", "info", category="steam")
            continue

        # Sell strategy 3: ratio guard
        if sell_strategy == 3:
            buy_record = _find_buy_record(purchases_snapshot, aid, market_hash_name)
            if buy_record:
                buy_price = float(buy_record.get("price") or 0)
                market_price_at_buy = float(buy_record.get("market_price") or 0)
                if buy_price > 0 and market_price_at_buy > 0 and list_price > 0:
                    current_ratio = buy_price / (list_price / 1.15)
                    original_ratio = buy_price / (market_price_at_buy / 1.15)
                    ratio_multiplier = float(pipeline_cfg.get("profit_ratio_multiplier", 1.05) or 1.05)
                    ratio_limit = original_ratio * ratio_multiplier
                    profit_output = {
                        "current_ratio": current_ratio,
                        "original_ratio": original_ratio,
                        "ratio_limit": ratio_limit,
                    }
                    if current_ratio > ratio_limit:
                        ctx.log(
                            f"[出售] 策略3不满足: {name} 当前买入/挂刀价比例({current_ratio:.4f}) "
                            f"高于 购入时买入/市场底价比例的{ratio_multiplier:.2f}倍({ratio_limit:.4f})，避免过低价格售出，跳过",
                            "info", category="steam",
                        )
                        continue
                    ctx.log(
                        f"[出售] 策略3满足: {name} 当前买入/挂刀价比例({current_ratio:.4f}) <= {ratio_limit:.4f}，允许出售",
                        "info", category="steam",
                    )

        # Cost floor: never list below what the item cost us (plus margin).
        cost_floor_output = {}
        if cost_floor_enabled and buy_record:
            breach = _cost_floor_breach(buy_record.get("price"), list_price, cost_floor_ratio)
            if breach is not None:
                net_proceeds, required_price = breach
                cost_floor_output = {
                    "cost": float(buy_record.get("price") or 0),
                    "net_proceeds": round(net_proceeds, 2),
                    "required_price": required_price,
                    "ratio": cost_floor_ratio,
                }
                ctx.log(
                    f"[出售] {name} assetid={aid} 触及成本底线：挂价 {list_price:.2f} 到手约 "
                    f"{net_proceeds:.2f} 低于成本×{cost_floor_ratio:g}，需 {required_price:.2f}，跳过等待回升",
                    "info", category="steam",
                )
                continue

        custom_outputs = {
            "guard.sell_cost_floor": cost_floor_output,
            "guard.sell_liquidity_cap": liquidity_output,
            "guard.max_listings_per_item": {
                "steam_same_name": steam_same_name,
                "round_same_name": already_in_this_round,
                "max_per_item": max_per_item,
            },
            "pricing.steam_wall_price": {
                "list_price": list_price,
                "reason": reason,
                "order_count": len(orders_data.get("sell_orders") or []),
                "min_lowest_tier_volume": min_tier_volume,
                "max_price_ratio": max_price_ratio,
                "anchor_price": signals["anchor_price"],
                "anchor_max_ratio": anchor_max_ratio,
            },
            "pricing.steam_wall_gap": {
                "list_price": list_price,
                "reason": reason,
                "order_count": len(orders_data.get("sell_orders") or []),
                "min_lowest_tier_volume": min_tier_volume,
                "max_price_ratio": max_price_ratio,
                "anchor_price": signals["anchor_price"],
                "anchor_max_ratio": anchor_max_ratio,
            },
            "pricing.price_offset": {"sell_price_offset": sell_offset},
            "pricing.price_ceiling": {
                "min_lowest_tier_volume": min_tier_volume,
                "max_price_ratio": max_price_ratio,
            },
            "guard.rising_trend_wait": {"trend": trend, "trend_days": trend_days},
            "guard.profit_ratio": profit_output,
        }
        custom_context = {
            "item": it,
            "buy_record": buy_record or {},
            "listing": {"list_price": list_price, "display_price": display_price},
            "config": cfg,
        }
        custom_results, blocking = evaluate_strategy_runtime_modules(
            cfg,
            "sell",
            "sell.listing_guard",
            context=custom_context,
            outputs=custom_outputs,
        )
        for result in custom_results:
            level = "warn" if result.get("status") in {"reject", "error"} else "info"
            ctx.log(
                f"[策略模块] {name} assetid={aid} {result.get('module_name')}: "
                f"{result.get('reason')} ({result.get('status')})",
                level,
                category="steam",
            )
        if blocking:
            continue

        price_cents = list_price_display_to_cents(display_price, account_currency)
        to_list_by_name[market_hash_name] += 1
        n_this_name = to_list_by_name[market_hash_name]
        ctx.log(
            f"[出售] 列入待上架 assetid={aid} {name} 价格={list_price:.2f}"
            f"（该同名 Steam 在售 {steam_same_name}，本轮回第 {n_this_name} 件，上限 {max_per_item}）",
            "info", category="steam",
        )
        to_list.append({
            "it": it, "list_price": list_price, "reason": reason,
            "price_cents": price_cents, "market_hash_name": market_hash_name,
            "name": name, "aid": aid,
        })

    if skip_same_name_cap:
        total_skip = sum(skip_same_name_cap.values())
        parts = [f"{n} x{c}" for n, c in sorted(skip_same_name_cap.items(), key=lambda x: -x[1])[:5]]
        if len(skip_same_name_cap) > 5:
            parts.append(f"等共 {len(skip_same_name_cap)} 种")
        ctx.log(f"[出售] 同名在售已达上限跳过 共 {total_skip} 件（{', '.join(parts)}）", "info", category="steam")

    if skip_liquidity_cap:
        total_skip = sum(skip_liquidity_cap.values())
        parts = [f"{n} x{c}" for n, c in sorted(skip_liquidity_cap.items(), key=lambda x: -x[1])[:5]]
        if len(skip_liquidity_cap) > 5:
            parts.append(f"等共 {len(skip_liquidity_cap)} 种")
        ctx.log(f"[出售] 卖出侧流动性限制跳过 共 {total_skip} 件（{', '.join(parts)}）", "info", category="steam")

    return to_list


def _find_buy_record(purchases_snapshot: list, aid: str, market_hash_name: str) -> Optional[dict]:
    """Return the current purchase record that owns *aid*.

    A market name is not an ownership identifier: the inventory may contain a
    personal drop or a later acquisition with the same name as an old purchase.
    Automatic listing therefore fails closed unless a current, exact asset-id
    record exists.
    """
    aid = str(aid or "").strip()
    if not aid:
        return None

    for p in purchases_snapshot:
        if str(p.get("assetid") or "").strip() != aid:
            continue

        # Do not let stale/finished records authorize another listing.  These
        # checks also protect against a stale inventory snapshot immediately
        # after a listing or sale.
        sale_price = p.get("sale_price")
        if sale_price is not None:
            try:
                if float(sale_price) > 0:
                    continue
            except (TypeError, ValueError):
                continue
        if p.get("sold_at") not in (None, "", 0, 0.0):
            continue
        if p.get("pending_receipt") or p.get("listing"):
            continue
        return p
    return None


def _listing_response_body_preview(value: object, limit: int = 160) -> str:
    """Return a compact, bounded response excerpt suitable for logs."""
    if value is None:
        return "<empty>"
    if isinstance(value, bytes):
        text = value.decode("utf-8", errors="replace")
    else:
        text = str(value)
    source_was_truncated = len(text) > max(limit * 4, 640)
    if source_was_truncated:
        text = text[: max(limit * 4, 640)]
    text = " ".join(text.split())
    if not text:
        return "<empty>"
    text = re.sub(
        r"(?i)\b(bearer)\s+[a-z0-9._~+/=-]+",
        r"\1 ***",
        text,
    )
    text = re.sub(
        (
            r"(?i)\b("
            r"steamloginsecure|sessionid|access[_-]?token|"
            r"refresh[_-]?token|token|cookie"
            r")(\s*[\"']?\s*[:=]\s*[\"']?)([^\"'\s,;&}<]+)"
        ),
        r"\1\2***",
        text,
    )
    if source_was_truncated or len(text) > limit:
        return text[: limit - 3] + "..."
    return text


def _parse_listing_response(out: object) -> tuple[Optional[dict], str]:
    """Validate a low-level listing response and return diagnostics on failure."""
    if out is None:
        return None, "无响应"
    if not isinstance(out, dict):
        return None, f"返回类型异常: {type(out).__name__}"

    status_code = out.get("status_code")
    status_label = str(status_code) if status_code is not None else "unknown"
    raw_text = out.get("text")
    _, data = parse_sell_response(raw_text)
    if data is None:
        return (
            None,
            f"HTTP {status_label}; body={_listing_response_body_preview(raw_text)}",
        )
    return data, ""


def _listing_response_message(data: dict, limit: int = 80) -> str:
    value = data.get("message")
    if value in (None, ""):
        return ""
    return _listing_response_body_preview(value, limit=limit)


def _listing_response_is_success(out: object, data: dict, message: str) -> bool:
    """Accept success only from the expected Steam HTTP response contract."""
    if not isinstance(out, dict) or out.get("status_code") != 200:
        return False
    message_lower = message.lower()
    return (
        data.get("success") is True
        or "pending confirmation" in message_lower
        or "already have a listing" in message_lower
    )


def _listing_rate_limited(ctx: PipelineContext, out: object) -> bool:
    if not isinstance(out, dict) or out.get("status_code") != 429:
        return False
    remaining = _listing_cooldown.defer(out.get("retry_after"))
    preview = _listing_response_body_preview(out.get("text"))
    ctx.log(
        f"[出售] 上架接口 HTTP 429; body={preview}，冷却约 {remaining:.0f}s，"
        "停止本批次，剩余物品未提交，不自动重试上架",
        "warn", category="steam",
    )
    return True


def _submit_listings(
    ctx: PipelineContext,
    to_list: list,
    session,
    session_id_effective: str,
    listing_delay: float,
) -> int:
    """POST listing requests to Steam and handle retries.

    Returns the count of successfully submitted listings.
    """
    remaining = _listing_cooldown.remaining()
    if remaining > 0:
        ctx.log(f"[出售] 上架接口冷却中，约 {remaining:.0f}s 后可重试，本批次未提交", "warn", category="steam")
        return 0
    listed = 0
    for entry in to_list:
        if ctx.is_stop_requested():
            ctx.set_status("stopped", "已停止")
            return listed

        it = entry["it"]
        list_price = entry["list_price"]
        reason = entry["reason"]
        price_cents = entry["price_cents"]
        name = entry["name"]
        aid = entry["aid"]

        ctx.log(f"[出售] 上架请求 {name} assetid={aid} 价格={list_price:.2f} ({reason})", "info", category="steam")

        def _do_list():
            return list_item(
                session, session_id_effective,
                int(it.get("appid", 730)),
                str(it.get("contextid") or "2"),
                str(it.get("assetid", "")),
                price_cents,
            )

        try:
            out = _do_list()
            if _listing_rate_limited(ctx, out):
                break
            data, response_error = _parse_listing_response(out)
            if data is None:
                ctx.log(
                    f"[出售] {name} assetid={aid} 上架响应异常: {response_error}",
                    "warn",
                    category="steam",
                )
                jittered_sleep(listing_delay)
                continue

            msg = _listing_response_message(data)
            msg_lower = msg.lower()

            if _listing_response_is_success(out, data, msg):
                listed += 1
                if "already have a listing" in msg_lower:
                    ctx.log(
                        f"[出售] {name} assetid={aid} Steam 提示已存在挂单，价格 {list_price:.2f} 可能未生效，请在售列表核对",
                        "warn", category="steam",
                    )
                ctx.log(f"[出售] 已上架 assetid={aid} {name} 价格={list_price:.2f} ({reason})", "info", category="steam")
                _record_listing_success(ctx, aid, name, list_price, listing_delay)
                continue

            if "previous action completes" in msg_lower or "until your previous" in msg_lower:
                ctx.log(f"[出售] {name} assetid={aid} 前一操作未完成，等待 5s 后重试", "info", category="steam")
                jittered_sleep(5)
                out2 = _do_list()
                if _listing_rate_limited(ctx, out2):
                    break
                data2, retry_error = _parse_listing_response(out2)
                if data2 is None:
                    ctx.log(
                        f"[出售] {name} assetid={aid} 上架重试响应异常: {retry_error}",
                        "warn",
                        category="steam",
                    )
                else:
                    msg2 = _listing_response_message(data2).lower()
                    if _listing_response_is_success(out2, data2, msg2):
                        listed += 1
                        ctx.log(f"[出售] 已上架 assetid={aid} {name} 价格={list_price:.2f} ({reason}) [重试成功]", "info", category="steam")
                        _record_listing_success(ctx, aid, name, list_price, listing_delay)
                        continue

            response_preview = (
                _listing_response_body_preview(out.get("text"))
                if isinstance(out, dict)
                else type(out).__name__
            )
            ctx.log(f"[出售] 上架失败 assetid={aid} {name}: {msg or response_preview}", "warn", category="steam")
        except Exception as ex:
            error_detail = _listing_response_body_preview(ex)
            ctx.log(f"[出售] 上架时发生未捕获异常 assetid={aid} {name}: {type(ex).__name__} - {error_detail}", "error", category="steam")

        jittered_sleep(listing_delay)

    return listed


def _auto_confirm_listings(ctx: PipelineContext, cfg: dict, steam_id: str, cookies: str) -> bool:
    """Confirm pending Steam Guard confirmations after listing, if configured.

    Returns ``True`` when confirmations were processed, meaning a freshly
    created listing should be visible on Steam right away.
    """
    steam_confirm_cfg = cfg.get("steam_confirm") or {}
    if not bool(steam_confirm_cfg.get("enabled")):
        return False
    identity_secret = (steam_confirm_cfg.get("identity_secret") or "").strip()
    device_id = (steam_confirm_cfg.get("device_id") or "").strip()
    if not identity_secret or not device_id:
        ctx.log("[确认] 已开启自动确认，但 identity_secret/device_id 未配置，跳过", "warn", category="steam")
        return False
    jittered_sleep(2)
    ctx.log("[确认] 正在检查待确认列表…", "info", category="steam")
    okc, n, errc = auto_confirm_once(
        identity_secret=identity_secret,
        device_id=device_id,
        steam_id=str(steam_id),
        cookies=str(cookies),
    )
    if okc:
        ctx.log(f"[确认] 已自动确认 {n} 项", "info", category="steam")
        return True
    ctx.log(f"[确认] 自动确认失败: {errc}", "warn", category="steam")
    return False


def _build_repricing_plan(
    ctx: PipelineContext,
    cfg: dict,
    session,
    pipeline_cfg: dict,
    purchases_snapshot: list,
    sales_snapshot: list,
    active_listing_ids: set,
    listing_assetid_to_name: dict,
    account_currency: str,
    rate_map: dict,
) -> list:
    """Decide which currently listed items should be delisted and relisted.

    Only listings whose current price and age can be traced back to a local
    listing record are considered; anything else is left untouched.
    """
    if not bool(pipeline_cfg.get("sell_reprice_enabled", True)):
        return []
    min_drop_pct = float(pipeline_cfg.get("sell_reprice_min_drop_pct", 5) or 0)
    max_age_hours = float(pipeline_cfg.get("sell_reprice_max_age_hours", 72) or 0)
    min_interval_hours = float(pipeline_cfg.get("sell_reprice_min_interval_hours", 6) or 0)
    cost_floor_enabled = bool(pipeline_cfg.get("sell_cost_floor_enabled", True))
    try:
        cost_floor_ratio = float(pipeline_cfg.get("sell_cost_floor_ratio", 1.0))
    except (TypeError, ValueError):
        cost_floor_ratio = 1.0
    params = _pricing_params(pipeline_cfg)
    trend_days = int(pipeline_cfg.get("sell_trend_days", 7))
    anchor_max_ratio = params["anchor_max_ratio"]
    anchor_days = params["anchor_days"]
    try:
        anchor_enabled = float(anchor_max_ratio) > 0
    except (TypeError, ValueError):
        anchor_enabled = False
    history_cache: dict = {}

    owned_by_assetid: dict = {}
    for p in purchases_snapshot:
        aid = str(p.get("assetid") or "").strip()
        if not aid:
            continue
        try:
            if float(p.get("sale_price") or 0) > 0:
                continue
        except (TypeError, ValueError):
            continue
        if p.get("sold_at") not in (None, "", 0, 0.0):
            continue
        owned_by_assetid[aid] = p

    latest_by_assetid = _latest_listing_records(sales_snapshot)
    now = time.time()
    to_reprice = []
    for raw_aid in sorted(str(x or "").strip() for x in (active_listing_ids or set())):
        aid = raw_aid
        if not aid or aid not in owned_by_assetid:
            continue
        purchase = owned_by_assetid[aid]
        name = (purchase.get("name") or "").strip()
        market_hash_name = (
            listing_assetid_to_name.get(aid)
            or purchase.get("market_hash_name")
            or name
        ).strip()
        if not market_hash_name:
            continue
        record = latest_by_assetid.get(aid)
        if not record:
            continue
        try:
            current_price = float(record.get("price") or 0)
        except (TypeError, ValueError):
            continue
        listed_at = float(record.get("at") or 0)
        age_hours = (now - listed_at) / 3600 if listed_at > 0 else None
        record_kind = str(record.get("kind") or "list")
        if (
            min_interval_hours > 0
            and record_kind == "reprice"
            and age_hours is not None
            and age_hours < min_interval_hours
        ):
            ctx.debug(f"[重挂] {name} assetid={aid} 距上次重挂不足 {min_interval_hours:g} 小时，跳过", category="steam")
            continue

        recent_sale_price = None
        if anchor_enabled:
            recent_sale_price = _history_signals_cached(
                history_cache, market_hash_name, trend_days, anchor_days
            )["anchor_price"]
        target_price, price_reason, orders_data, orders_error = _fetch_steam_target_price(
            session, market_hash_name, int(purchase.get("appid", 730) or 730), params,
            recent_sale_price=recent_sale_price,
            anchor_max_ratio=anchor_max_ratio,
        )
        if orders_data is None:
            ctx.debug(f"[重挂] {name} assetid={aid} {orders_error}，保持现状", category="steam")
            continue
        if target_price is None or float(target_price) <= 0:
            ctx.debug(f"[重挂] {name} assetid={aid} 无法计算定价({price_reason})，保持现状", category="steam")
            continue
        target_price = round(float(target_price), 2)

        reprice_reason = _should_reprice(
            current_price, target_price, age_hours,
            min_drop_pct, max_age_hours, min_interval_hours, record_kind,
        )
        if not reprice_reason:
            continue

        if cost_floor_enabled:
            breach = _cost_floor_breach(purchase.get("price"), target_price, cost_floor_ratio)
            if breach is not None:
                net_proceeds, _required = breach
                ctx.log(
                    f"[重挂] {name} assetid={aid} 触及成本底线：新价 {target_price:.2f} 到手约 "
                    f"{net_proceeds:.2f} 低于成本×{cost_floor_ratio:g}，保留现价",
                    "info", category="steam",
                )
                continue

        display_price = target_price
        if account_currency != "CNY":
            rate = rate_map.get(account_currency)
            if not rate:
                ctx.log(
                    f"[重挂] {name} assetid={aid} 账号币种={account_currency} 缺少汇率，跳过重挂",
                    "error", category="steam",
                )
                continue
            display_price = round(target_price / rate, 2)

        to_reprice.append({
            "aid": aid,
            "name": name,
            "market_hash_name": market_hash_name,
            "appid": int(purchase.get("appid", 730) or 730),
            "contextid": str(purchase.get("contextid") or "2"),
            "list_price": target_price,
            "old_price": current_price,
            "age_hours": age_hours,
            "price_cents": list_price_display_to_cents(display_price, account_currency),
            "reason": reprice_reason,
        })
        ctx.log(
            f"[重挂] {name} assetid={aid} 现价={current_price:.2f} 新价={target_price:.2f}（{reprice_reason}）",
            "info", category="steam",
        )
    return to_reprice


def _reprice_listings(
    ctx: PipelineContext,
    entries: list,
    session,
    session_id_effective: str,
    listing_delay: float,
) -> int:
    """Delist and relist the given entries, returning how many were relisted."""
    remaining = _listing_cooldown.remaining()
    if remaining > 0:
        ctx.log(f"[重挂] 上架接口冷却中，约 {remaining:.0f}s 后可重试，本轮未重挂", "warn", category="steam")
        return 0
    repriced = 0
    for entry in entries:
        if ctx.is_stop_requested():
            ctx.set_status("stopped", "已停止")
            return repriced

        aid = entry["aid"]
        name = entry["name"]
        list_price = entry["list_price"]
        price_cents = entry["price_cents"]
        reason = entry["reason"]
        appid = int(entry.get("appid", 730))
        contextid = str(entry.get("contextid") or "2")

        ctx.log(f"[重挂] 下架旧挂单 {name} assetid={aid} 现价={entry['old_price']:.2f}", "info", category="steam")

        def _log_fn(msg, level="info"):
            ctx.log(f"[重挂] {msg}", level, category="steam")

        try:
            ok, new_assetid, err = delist_item(aid, name, log_fn=_log_fn)
        except Exception as ex:
            ctx.log(
                f"[重挂] 下架异常 assetid={aid} {name}: {type(ex).__name__} - {_listing_response_body_preview(ex)}",
                "error", category="steam",
            )
            continue
        if not ok:
            ctx.log(f"[重挂] 下架失败 assetid={aid} {name}: {err or '未知原因'}", "warn", category="steam")
            continue
        if err:
            ctx.log(f"[重挂] 下架提示 assetid={aid} {name}: {err}", "warn", category="steam")

        # 下架后 Steam 会分配新的 assetid，先本地解绑再以新 assetid 重新上架
        _rebind_purchase_assetid(ctx, aid, new_assetid)
        if not new_assetid:
            ctx.log(
                f"[重挂] 中止 {name}：已下架但未取得新 assetid，物品已回到库存；"
                "本地资产号已清空，请用「同步售出/持有」补全后再上架",
                "warn", category="steam",
            )
            continue

        try:
            out = list_item(session, session_id_effective, appid, contextid, new_assetid, price_cents)
        except Exception as ex:
            ctx.log(
                f"[重挂] 上架异常 assetid={new_assetid} {name}: {type(ex).__name__} - {_listing_response_body_preview(ex)}",
                "error", category="steam",
            )
            continue
        if _listing_rate_limited(ctx, out):
            break

        data, response_error = _parse_listing_response(out)
        if data is None:
            ctx.log(f"[重挂] 上架响应异常 assetid={new_assetid} {name}: {response_error}", "warn", category="steam")
            jittered_sleep(listing_delay)
            continue

        msg = _listing_response_message(data)
        if _listing_response_is_success(out, data, msg):
            repriced += 1
            ctx.log(
                f"[重挂] 成功 assetid={new_assetid} {name} 价格={list_price:.2f}（{reason}）",
                "info", category="steam",
            )
            _record_listing_success(ctx, new_assetid, name, list_price, listing_delay, kind="reprice")
            continue

        ctx.log(
            f"[重挂] 上架失败 assetid={new_assetid} {name}: {msg or _listing_response_body_preview(out.get('text'))}",
            "warn", category="steam",
        )
        jittered_sleep(listing_delay)
    return repriced


def _reconcile_listings(
    ctx: PipelineContext,
    entries: list,
    cookies: str,
    auto_confirmed: bool,
    debug_fn=None,
) -> int:
    """Verify that the listings we just recorded are actually live on Steam.

    Returns the number of recorded listings that could not be found.  When
    automatic confirmation ran the missing ones are marked as failed so the
    next round retries them; otherwise they are only reported, because the
    listing is most likely still waiting for a manual confirmation.
    """
    expected: dict = {}
    for entry in entries or []:
        aid = str((entry.get("it") or {}).get("assetid") or entry.get("aid") or "").strip()
        if aid:
            expected[aid] = entry
    if not expected:
        return 0

    ok, active_ids, err, _names = fetch_my_listings(cookies, debug_fn=debug_fn)
    if not ok:
        ctx.log(f"[对账] 上架后核对失败: {err or '未知原因'}，跳过核对", "warn", category="steam")
        return 0

    purchases_by_assetid = {
        str(p.get("assetid") or "").strip(): p
        for p in ctx.state.get_purchases()
        if str(p.get("assetid") or "").strip()
    }
    missing = []
    for aid, entry in expected.items():
        if aid in active_ids:
            continue
        record = purchases_by_assetid.get(aid)
        if not record or not record.get("listing"):
            # 该件本次并未成功上架（_submit_listings 已记录原因），无需对账
            continue
        missing.append(aid)

    if not missing:
        ctx.debug(f"[对账] 本轮 {len(expected)} 件均已在 Steam 在售列表中", category="steam")
        return 0

    for aid in missing:
        entry = expected[aid]
        name = entry.get("name") or ""
        if auto_confirmed:
            ctx.log(
                f"[对账] {name} assetid={aid} 已记录上架但未出现在 Steam 在售列表，标记为异常待重试",
                "warn", category="steam",
            )
            _update_purchase_for_assetid(ctx, aid, {"listing": False, "listing_status": "error"})
        else:
            ctx.log(
                f"[对账] {name} assetid={aid} 未出现在 Steam 在售列表，可能仍待手动确认，请核对",
                "warn", category="steam",
            )
    return len(missing)


def _run_reprice_phase(
    ctx: PipelineContext,
    cfg: dict,
    session,
    session_id_effective: str,
    pipeline_cfg: dict,
    purchases_snapshot: list,
    ok_listings: bool,
    active_listing_ids: set,
    listing_assetid_to_name: dict,
    account_currency: str,
    rate_map: dict,
    listing_delay: float,
) -> int:
    """Reprice stale or overpriced live listings; returns how many were relisted."""
    if not ok_listings or not active_listing_ids:
        return 0
    to_reprice = _build_repricing_plan(
        ctx, cfg, session, pipeline_cfg, purchases_snapshot, ctx.state.get_sales(),
        active_listing_ids, listing_assetid_to_name, account_currency, rate_map,
    )
    if not to_reprice:
        return 0
    if not bool((cfg.get("steam_confirm") or {}).get("enabled")):
        ctx.log("[重挂] 未开启自动确认：重挂后请在 Steam 手机端确认新的上架请求", "warn", category="steam")
    ctx.log(f"[重挂] 开始重挂 {len(to_reprice)} 件", "info", category="steam")
    repriced = _reprice_listings(ctx, to_reprice, session, session_id_effective, listing_delay)
    if repriced:
        ctx.log(f"[重挂] 本轮重挂完成 {repriced} 件", "info", category="steam")
    return repriced


# ---------------------------------------------------------------------------
# Sell phase orchestrator
# ---------------------------------------------------------------------------

def _run_sell_phase_impl(cfg: dict, state, flow_id: str, items: Optional[list] = None) -> None:
    cfg = apply_strategy_to_config(cfg, "sell")
    pipeline_cfg = cfg.get("pipeline", {})
    verbose = bool(pipeline_cfg.get("verbose_debug", False))
    ctx = PipelineContext(state, flow_id, verbose=verbose)
    sell_strategy = int(pipeline_cfg.get("sell_strategy", 1))

    if sell_strategy == 4:
        ctx.log("策略4 暂停自动出售，跳过", "info")
        return

    cred_steam = get_steam_credentials()
    session_result = _resolve_steam_session(ctx, cred_steam)
    if session_result is None:
        return
    session, session_id_effective = session_result

    items = _get_inventory(ctx, items)
    if items is None:
        return

    sellable = [it for it in items if it.get("can_sell")]
    if not sellable:
        ctx.log("当前无可出售物品", "info", category="steam")
        return

    listing_delay = max(1, int(pipeline_cfg.get("listing_delay_seconds", 3) or 3))
    steam_debug = bool(pipeline_cfg.get("verbose_debug") or pipeline_cfg.get("steam_listings_debug"))
    debug_fn = (lambda m: ctx.log(m, "debug", category="steam")) if steam_debug else None

    ok_listings, active_listing_ids, err_listings, listing_assetid_to_name = fetch_my_listings(
        cred_steam.get("cookies"), debug_fn=debug_fn
    )
    if ok_listings:
        ctx.log(f"[出售] Steam 在售列表拉取成功，共 {len(active_listing_ids)} 个在售", "info", category="steam")
    else:
        ctx.log(f"[出售] Steam 在售列表拉取失败: {err_listings or '未知'}，同名在售校验将按本地购买记录", "warn", category="steam")

    purchases_snapshot = ctx.state.get_purchases()
    account = get_current_account()
    if account is None:
        # 工厂重置后账号列表为空，直接跳过出售避免以错误币种上架
        ctx.log("[出售] 无有效账号（可能刚执行了出厂重置），跳过本次出售", "warn", category="steam")
        return
    account_currency_cached = (account.get("currency_code") or "").strip().upper()
    region_check = refresh_account_region_currency(
        account.get("id"),
        cookies_raw=cred_steam.get("cookies", ""),
    )
    if not region_check.get("ok"):
        ctx.log(
            "[出售] 无法实时确认 Steam 账号结算币种，已拒绝上架，"
            f"防止跨区价格误卖。原因: {region_check.get('error') or '未知原因'}",
            "error",
            category="steam",
        )
        return
    account_currency = (region_check.get("currency_code") or "").strip().upper()
    account_region = (region_check.get("region_code") or "").strip().upper()
    if not account_currency:
        ctx.log(
            "[出售] Steam 未返回结算币种，已拒绝上架，防止跨区价格误卖。",
            "error",
            category="steam",
        )
        return
    if account_currency_cached and account_currency_cached != account_currency:
        ctx.log(
            f"[出售] 检测到账号币种变化: 缓存={account_currency_cached} 实时={account_currency}，"
            "已更新并使用实时币种定价。",
            "warn",
            category="steam",
        )
    if account_region:
        ctx.debug(
            f"[出售] 账号地区按结算币种派生: 币种={account_currency} 地区={account_region}",
            category="steam",
        )
    rate_map = _load_rate_map()

    assetid_to_name_map = {
        str(p.get("assetid") or "").strip(): (p.get("market_hash_name") or p.get("name") or "").strip()
        for p in purchases_snapshot if str(p.get("assetid") or "").strip()
    }

    ctx.log(
        f"策略{sell_strategy}，可出售 {len(sellable)} 件"
        f"（智能定价：墙+断层；上架间隔={listing_delay}s）",
        "info", category="steam",
    )

    to_list = _build_listing_plan(
        ctx, cfg, session, sellable, sell_strategy, pipeline_cfg,
        purchases_snapshot, ok_listings, active_listing_ids,
        listing_assetid_to_name, assetid_to_name_map, account_currency, rate_map,
    )

    listed = 0
    if to_list:
        ctx.log(f"[出售] 开始上架 {len(to_list)} 件", "info", category="steam")
        listed = _submit_listings(ctx, to_list, session, session_id_effective, listing_delay)
        if listed:
            ctx.log(f"[出售] 本轮回共上架 {listed} 件，等待下一轮", "info", category="steam")
    else:
        ctx.debug("[出售] 本轮回无需上架", category="steam")

    repriced = _run_reprice_phase(
        ctx, cfg, session, session_id_effective, pipeline_cfg,
        purchases_snapshot, ok_listings, active_listing_ids,
        listing_assetid_to_name, account_currency, rate_map, listing_delay,
    )

    auto_confirmed = False
    if listed or repriced:
        auto_confirmed = _auto_confirm_listings(
            ctx, cfg, cred_steam.get("steam_id", ""), cred_steam.get("cookies", "")
        )

    if listed and bool(pipeline_cfg.get("sell_reconcile_enabled", True)):
        _reconcile_listings(
            ctx, to_list, cred_steam.get("cookies", ""), auto_confirmed, debug_fn=debug_fn
        )


def _run_sell_phase(cfg: dict, state, flow_id: str, items: Optional[list] = None) -> None:
    if not _sell_phase_lock.acquire(blocking=False):
        state.log("[出售] 已有出售任务在执行，本次跳过", "info", category="steam", flow_id=flow_id)
        return
    try:
        _run_sell_phase_impl(cfg, state, flow_id, items)
    finally:
        _sell_phase_lock.release()


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_sell_phase_on_inventory_update(items: list) -> None:
    cfg = merge(DEFAULTS, load_app_config_validated())
    state = get_state()
    t = threading.Thread(
        target=_run_sell_phase,
        args=(cfg, state, "inventory"),
        kwargs={"items": items},
        daemon=True,
    )
    t.start()
