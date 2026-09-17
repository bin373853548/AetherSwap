"""售卖侧守卫回归测试：成本底线、卖出侧流动性限制、成交锚定价、上架后对账。"""

import copy
from unittest.mock import MagicMock

import pytest

from app.config_schema import DEFAULTS


# ------------------------------------------------------------------ helpers --

def _ctx(logged=None):
    ctx = MagicMock()
    ctx.is_stop_requested.return_value = False
    ctx.debug.return_value = None
    ctx.state = MagicMock()

    def _log(msg, level="info", **kw):
        if logged is not None:
            logged.append((level, msg))

    ctx.log.side_effect = _log
    return ctx


def _orders():
    # 两档：10.00×4 / 11.00×20，断层跳跃到 10.99
    return {"sell_orders": [(10.0, 4), (11.0, 20)]}


def _sellable(assetid="111", name="AK-47 | Redline"):
    return [{
        "name": name,
        "market_hash_name": name,
        "assetid": assetid,
        "can_sell": True,
        "appid": 730,
        "contextid": "2",
    }]


def _plan_args(**overrides):
    args = dict(
        ctx=_ctx([]),
        cfg={"pipeline": {}},
        session=MagicMock(),
        sellable=_sellable(),
        sell_strategy=1,
        pipeline_cfg={"sell_cost_floor_enabled": False, "sell_anchor_max_ratio": 0},
        purchases_snapshot=[{"assetid": "111", "market_hash_name": "AK-47 | Redline", "price": 5.0}],
        ok_listings=False,
        active_listing_ids=set(),
        listing_assetid_to_name={},
        assetid_to_name_map={},
        account_currency="CNY",
        rate_map={},
    )
    args.update(overrides)
    return args


@pytest.fixture()
def pricing(monkeypatch):
    """Patch the order book + price computation used by _build_listing_plan."""
    from app import sell_pipeline

    monkeypatch.setattr(sell_pipeline, "get_sell_orders_cny", lambda *_a, **_k: _orders())
    monkeypatch.setattr(sell_pipeline, "compute_smart_list_price", lambda *_a, **_k: (10.99, "断层跳跃"))
    return sell_pipeline


# ------------------------------------------------------- 成交锚定价（定价层）--

def test_sale_anchor_caps_gap_jump_price():
    from steam.market_orders import compute_smart_list_price

    price, _ = compute_smart_list_price([(10.0, 4), (11.0, 20)])
    assert price == 10.99

    capped, reason = compute_smart_list_price(
        [(10.0, 4), (11.0, 20)], recent_sale_price=9.0, anchor_max_ratio=1.05
    )
    assert capped == 9.45
    assert "成交锚" in reason


def test_sale_anchor_applies_to_single_tier_book():
    from steam.market_orders import compute_smart_list_price

    capped, reason = compute_smart_list_price(
        [(10.0, 5)], recent_sale_price=8.0, anchor_max_ratio=1.05
    )
    assert capped == 8.4
    assert "成交锚" in reason


@pytest.mark.parametrize(
    ("median", "ratio"),
    [(None, 1.05), (9.0, None), (9.0, 0), ("x", 1.05), (9.0, "x")],
)
def test_sale_anchor_disabled_inputs_leave_price_unchanged(median, ratio):
    from steam.market_orders import compute_smart_list_price

    price, reason = compute_smart_list_price(
        [(10.0, 4), (11.0, 20)], recent_sale_price=median, anchor_max_ratio=ratio
    )
    assert price == 10.99
    assert "成交锚" not in reason


def test_sale_anchor_ceiling_respects_min_floor():
    from steam.market_orders import STEAM_MIN_PRICE, _sale_anchor_ceiling

    assert _sale_anchor_ceiling(0.01, 1.05, STEAM_MIN_PRICE) == STEAM_MIN_PRICE
    assert _sale_anchor_ceiling(12.0, 1.05, STEAM_MIN_PRICE) == 12.6
    assert _sale_anchor_ceiling(None, 1.05, STEAM_MIN_PRICE) is None


# ------------------------------------------------------------- 成本底线 --

def test_cost_floor_breach_detects_loss_making_price():
    from app.sell_pipeline import _cost_floor_breach

    assert _cost_floor_breach(10.0, 12.5, 1.0) is None
    net, required = _cost_floor_breach(10.0, 10.0, 1.0)
    assert net < 10.0
    assert required == 11.5
    assert _cost_floor_breach(10.0, 10.0, 0) is None
    assert _cost_floor_breach(None, 10.0, 1.0) is None
    assert _cost_floor_breach("x", 10.0, 1.0) is None


def test_listing_plan_blocks_below_cost(pricing):
    logged = []
    result = pricing._build_listing_plan(**_plan_args(
        ctx=_ctx(logged),
        pipeline_cfg={"sell_cost_floor_enabled": True, "sell_cost_floor_ratio": 1.0},
        purchases_snapshot=[{"assetid": "111", "market_hash_name": "AK-47 | Redline", "price": 20.0}],
    ))

    assert result == []
    assert any("成本底线" in msg for _, msg in logged)


def test_listing_plan_allows_above_cost(pricing):
    result = pricing._build_listing_plan(**_plan_args(
        pipeline_cfg={"sell_cost_floor_enabled": True, "sell_cost_floor_ratio": 1.0},
        purchases_snapshot=[{"assetid": "111", "market_hash_name": "AK-47 | Redline", "price": 5.0}],
    ))
    assert len(result) == 1


def test_listing_plan_cost_floor_can_be_disabled(pricing):
    result = pricing._build_listing_plan(**_plan_args(
        pipeline_cfg={"sell_cost_floor_enabled": False},
        purchases_snapshot=[{"assetid": "111", "market_hash_name": "AK-47 | Redline", "price": 200.0}],
    ))
    assert len(result) == 1


# --------------------------------------------------- 卖出侧流动性限制 --

def _volume_signals(daily_volume):
    return {
        "latest_price": None,
        "trend": None,
        "median_price": None,
        "anchor_price": None,
        "daily_volume": daily_volume,
        "sample_size": 0,
    }


def test_listing_plan_caps_listings_by_daily_volume(pricing, monkeypatch):
    logged = []
    monkeypatch.setattr(pricing, "_steam_history_signals", lambda *a, **k: _volume_signals(20.0))

    result = pricing._build_listing_plan(**_plan_args(
        ctx=_ctx(logged),
        pipeline_cfg={"sell_liquidity_ratio": 0.05, "sell_anchor_max_ratio": 0},
        ok_listings=True,
        active_listing_ids={"other1"},
        listing_assetid_to_name={"other1": "AK-47 | Redline"},
    ))

    assert result == []
    assert any("卖出侧流动性限制" in msg for _, msg in logged)


def test_listing_plan_skips_liquidity_cap_when_disabled(pricing, monkeypatch):
    monkeypatch.setattr(pricing, "_steam_history_signals", lambda *a, **k: _volume_signals(20.0))

    result = pricing._build_listing_plan(**_plan_args(
        pipeline_cfg={"sell_liquidity_ratio": 0, "sell_anchor_max_ratio": 0},
        ok_listings=True,
        active_listing_ids={"other1"},
        listing_assetid_to_name={"other1": "AK-47 | Redline"},
    ))
    assert len(result) == 1


def test_listing_plan_skips_liquidity_cap_without_volume(pricing, monkeypatch):
    monkeypatch.setattr(pricing, "_steam_history_signals", lambda *a, **k: _volume_signals(0.0))

    result = pricing._build_listing_plan(**_plan_args(
        pipeline_cfg={"sell_liquidity_ratio": 0.05, "sell_anchor_max_ratio": 0},
        ok_listings=True,
        active_listing_ids={"other1"},
        listing_assetid_to_name={"other1": "AK-47 | Redline"},
    ))
    assert len(result) == 1


def test_listing_plan_passes_anchor_into_pricing(pricing, monkeypatch):
    captured = {}

    def _fake_price(*_a, **kwargs):
        captured.update(kwargs)
        return 10.99, "断层跳跃"

    monkeypatch.setattr(pricing, "compute_smart_list_price", _fake_price)
    monkeypatch.setattr(
        pricing, "_steam_history_signals",
        lambda *a, **k: dict(_volume_signals(50.0), anchor_price=9.0),
    )

    pricing._build_listing_plan(**_plan_args(
        pipeline_cfg={"sell_anchor_max_ratio": 1.05, "sell_anchor_days": 3},
    ))

    assert captured["recent_sale_price"] == 9.0
    assert captured["anchor_max_ratio"] == 1.05


# ------------------------------------------------------- 历史信号解析 --

def _history_rows():
    # 7 天窗口内的 3 天成交：价格 10/12/14，成交量 10/20/30
    return {
        "history": [
            ["Jan 10 2026 12: +0", 14.0, "30"],
            ["Jan 09 2026 12: +0", 12.0, "20"],
            ["Jan 08 2026 12: +0", 10.0, "10"],
            ["Jan 01 2026 12: +0", 8.0, "5"],
        ],
        "currency": "CNY",
    }


def test_history_signals_summarise_recent_trades(monkeypatch):
    from app.services import steam_client
    from app import sell_pipeline

    monkeypatch.setattr(
        steam_client.SteamClient, "fetch_history",
        lambda self, *a, **k: _history_rows(),
    )
    signals = sell_pipeline._steam_history_signals("AK-47 | Redline", window_days=7, anchor_days=3)

    assert signals["latest_price"] == 14.0
    assert signals["median_price"] == 12.0
    assert signals["anchor_price"] == 14.0
    assert signals["sample_size"] == 3
    assert signals["daily_volume"] == pytest.approx(60 / 7)


def test_history_signals_degrade_on_missing_data(monkeypatch):
    from app.services import steam_client
    from app import sell_pipeline

    monkeypatch.setattr(steam_client.SteamClient, "fetch_history", lambda self, *a, **k: None)
    signals = sell_pipeline._steam_history_signals("AK-47 | Redline")
    assert signals == sell_pipeline._EMPTY_HISTORY_SIGNALS

    monkeypatch.setattr(steam_client.SteamClient, "fetch_history", lambda self, *a, **k: {"history": []})
    assert sell_pipeline._steam_history_signals("AK-47 | Redline") == sell_pipeline._EMPTY_HISTORY_SIGNALS


def test_history_signals_require_minimum_trades(monkeypatch):
    from app.services import steam_client
    from app import sell_pipeline

    monkeypatch.setattr(
        steam_client.SteamClient, "fetch_history",
        lambda self, *a, **k: {"history": [["Jan 10 2026 12: +0", 14.0, "30"]], "currency": "CNY"},
    )
    signals = sell_pipeline._steam_history_signals("AK-47 | Redline")
    assert signals["median_price"] is None
    assert signals["anchor_price"] is None


# --------------------------------------------------------- 上架后对账 --

def _reconcile_args(**overrides):
    args = dict(
        ctx=_ctx([]),
        entries=[{"name": "AK-47 | Redline", "aid": "111", "it": {"assetid": "111"}}],
        cookies="steamLoginSecure=x",
        auto_confirmed=True,
        debug_fn=None,
    )
    args.update(overrides)
    return args


def test_reconcile_marks_missing_listing(monkeypatch):
    from app import sell_pipeline

    logged = []
    ctx = _ctx(logged)
    ctx.state.get_purchases.return_value = [{"assetid": "111", "_db_id": 7, "listing": True}]
    monkeypatch.setattr(sell_pipeline, "fetch_my_listings", lambda *a, **k: (True, set(), "", {}))

    found = sell_pipeline._reconcile_listings(**_reconcile_args(ctx=ctx))

    assert found == 1
    ctx.state.update_purchase_by_id.assert_called_once_with(
        7, {"listing": False, "listing_status": "error"}
    )
    assert any("未出现在 Steam 在售列表" in msg for _, msg in logged)


def test_reconcile_only_warns_when_confirm_disabled(monkeypatch):
    from app import sell_pipeline

    logged = []
    ctx = _ctx(logged)
    ctx.state.get_purchases.return_value = [{"assetid": "111", "_db_id": 7, "listing": True}]
    monkeypatch.setattr(sell_pipeline, "fetch_my_listings", lambda *a, **k: (True, set(), "", {}))

    found = sell_pipeline._reconcile_listings(**_reconcile_args(ctx=ctx, auto_confirmed=False))

    assert found == 1
    ctx.state.update_purchase_by_id.assert_not_called()
    assert any("待手动确认" in msg for _, msg in logged)


def test_reconcile_ignores_absent_entries(monkeypatch):
    from app import sell_pipeline

    ctx = _ctx([])
    ctx.state.get_purchases.return_value = [{"assetid": "111", "_db_id": 7, "listing": True}]
    monkeypatch.setattr(sell_pipeline, "fetch_my_listings", lambda *a, **k: (True, {"111"}, "", {}))

    assert sell_pipeline._reconcile_listings(**_reconcile_args(ctx=ctx)) == 0
    ctx.state.update_purchase_by_id.assert_not_called()


def test_reconcile_skips_unrecorded_attempts(monkeypatch):
    from app import sell_pipeline

    ctx = _ctx([])
    ctx.state.get_purchases.return_value = [{"assetid": "111", "_db_id": 7, "listing": False}]
    monkeypatch.setattr(sell_pipeline, "fetch_my_listings", lambda *a, **k: (True, set(), "", {}))

    assert sell_pipeline._reconcile_listings(**_reconcile_args(ctx=ctx)) == 0
    ctx.state.update_purchase_by_id.assert_not_called()


def test_reconcile_reports_fetch_failure(monkeypatch):
    from app import sell_pipeline

    logged = []
    ctx = _ctx(logged)
    monkeypatch.setattr(sell_pipeline, "fetch_my_listings", lambda *a, **k: (False, set(), "未登录", {}))

    assert sell_pipeline._reconcile_listings(**_reconcile_args(ctx=ctx)) == 0
    assert any("上架后核对失败" in msg for _, msg in logged)


def test_auto_confirm_disabled_reports_not_confirmed(monkeypatch):
    from app import sell_pipeline

    ctx = _ctx([])
    assert sell_pipeline._auto_confirm_listings(ctx, {}, "1", "c") is False
    assert sell_pipeline._auto_confirm_listings(
        ctx, {"steam_confirm": {"enabled": True}}, "1", "c"
    ) is False


# ----------------------------------------------------------- 配置与策略 --

@pytest.mark.parametrize(
    "key",
    [
        "sell_reconcile_enabled",
        "sell_cost_floor_enabled",
        "sell_cost_floor_ratio",
        "sell_anchor_max_ratio",
        "sell_anchor_days",
        "sell_liquidity_ratio",
    ],
)
def test_new_sell_guard_defaults_exist(key):
    assert key in DEFAULTS["pipeline"]


def test_range_anchor_ratio_and_days_are_clamped():
    import warnings
    from app.config_schema import _validate_ranges, merge

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        cfg = _validate_ranges(merge(DEFAULTS, {"pipeline": {
            "sell_anchor_max_ratio": -1, "sell_anchor_days": 0, "sell_liquidity_ratio": -0.2,
        }}))

    assert cfg["pipeline"]["sell_anchor_max_ratio"] == 0
    assert cfg["pipeline"]["sell_anchor_days"] == 1
    assert cfg["pipeline"]["sell_liquidity_ratio"] == 0
    assert caught


def test_default_sell_strategies_include_cost_floor():
    from app import strategy_engine as se

    for sid in ("system.sell.immediate", "system.sell.trend", "system.sell.profit_guard"):
        steps = [s["module_id"] for s in se.get_strategy(sid)["steps"]]
        assert "guard.sell_cost_floor" in steps, sid


def test_apply_strategy_enables_cost_floor_and_passes_sell_knobs():
    from app import strategy_engine as se

    cfg = copy.deepcopy(DEFAULTS)
    cfg.setdefault("strategies", {})
    strategy = {
        "id": "custom.sell.test",
        "name": "test",
        "strategy_type": "sell",
        "steps": [
            {"module_id": "sell.sellable_inventory_filter", "enabled": True, "params": {}},
            {"module_id": "guard.max_listings_per_item", "enabled": True,
             "params": {"max_listings_per_item": 4, "sell_liquidity_ratio": 0.02}},
            {"module_id": "pricing.steam_wall_price", "enabled": True,
             "params": {"sell_anchor_max_ratio": 1.03, "sell_anchor_days": 5}},
            {"module_id": "guard.sell_cost_floor", "enabled": True,
             "params": {"sell_cost_floor_ratio": 1.1}},
            {"module_id": "action.steam_list", "enabled": True, "params": {}},
        ],
    }
    out = se.apply_strategy_to_config(cfg, "sell", strategy_override=strategy)
    pipe = out["pipeline"]

    assert pipe["sell_cost_floor_enabled"] is True
    assert pipe["sell_cost_floor_ratio"] == 1.1
    assert pipe["sell_liquidity_ratio"] == 0.02
    assert pipe["sell_anchor_max_ratio"] == 1.03
    assert pipe["sell_anchor_days"] == 5


def test_apply_strategy_without_cost_floor_disables_it():
    from app import strategy_engine as se

    cfg = copy.deepcopy(DEFAULTS)
    strategy = {
        "id": "custom.sell.nocost",
        "name": "test",
        "strategy_type": "sell",
        "steps": [
            {"module_id": "sell.sellable_inventory_filter", "enabled": True, "params": {}},
            {"module_id": "guard.max_listings_per_item", "enabled": True, "params": {}},
            {"module_id": "pricing.steam_wall_price", "enabled": True, "params": {}},
            {"module_id": "action.steam_list", "enabled": True, "params": {}},
        ],
    }
    out = se.apply_strategy_to_config(cfg, "sell", strategy_override=strategy)
    assert out["pipeline"]["sell_cost_floor_enabled"] is False
