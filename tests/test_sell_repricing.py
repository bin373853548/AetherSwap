"""Regression tests for listing repricing and duplicate-listing guard."""

import json
from unittest.mock import MagicMock

import pytest


# ------------------------------------------------------------------ helpers --

def _build_ctx(logged=None):
    ctx = MagicMock()
    ctx.is_stop_requested.return_value = False
    ctx.debug.return_value = None
    ctx.state = MagicMock()

    def _log(msg, level="info", **kw):
        if logged is not None:
            logged.append((level, msg))

    ctx.log.side_effect = _log
    return ctx


def _sellable(assetid="12345678", name="AK-47 | Redline"):
    return [{
        "name": name,
        "market_hash_name": name,
        "assetid": assetid,
        "can_sell": True,
        "appid": 730,
        "contextid": "2",
    }]


def _orders():
    return {"sell_orders": [{"price": 1200, "quantity": 5}]}


# --------------------------------------------------- duplicate listing guard --

def test_build_listing_plan_skips_assetid_already_listed():
    from app.sell_pipeline import _build_listing_plan

    logged = []
    ctx = _build_ctx(logged)
    called = []

    def _orders_stub(*_a, **_k):
        called.append(1)
        return _orders()

    import app.sell_pipeline as sp
    original = sp.get_sell_orders_cny
    sp.get_sell_orders_cny = _orders_stub
    try:
        result = _build_listing_plan(
            ctx=ctx,
            cfg={"pipeline": {}},
            session=MagicMock(),
            sellable=_sellable(),
            sell_strategy=1,
            pipeline_cfg={},
            purchases_snapshot=[{"assetid": "12345678", "market_hash_name": "AK-47 | Redline"}],
            ok_listings=True,
            active_listing_ids={"12345678"},
            listing_assetid_to_name={"12345678": "AK-47 | Redline"},
            assetid_to_name_map={},
            account_currency="CNY",
            rate_map={},
        )
    finally:
        sp.get_sell_orders_cny = original
        sp.compute_smart_list_price = sp.compute_smart_list_price

    assert result == []
    assert called == [], "已在架物品不应再拉取卖单或生成上架计划"
    assert any("已在 Steam 在售列表中" in msg for _, msg in logged)


def test_build_listing_plan_lists_when_not_in_steam_listings(monkeypatch):
    from app import sell_pipeline
    from app.sell_pipeline import _build_listing_plan

    ctx = _build_ctx([])
    monkeypatch.setattr(sell_pipeline, "get_sell_orders_cny", lambda *_a, **_k: _orders())
    monkeypatch.setattr(
        sell_pipeline, "compute_smart_list_price", lambda *_a, **_k: (10.0, "wall")
    )

    result = _build_listing_plan(
        ctx=ctx,
        cfg={"pipeline": {}},
        session=MagicMock(),
        sellable=_sellable(),
        sell_strategy=1,
        pipeline_cfg={},
        purchases_snapshot=[{"assetid": "12345678", "market_hash_name": "AK-47 | Redline"}],
        ok_listings=True,
        active_listing_ids={"99999999"},
        listing_assetid_to_name={"99999999": "Other"},
        assetid_to_name_map={},
        account_currency="CNY",
        rate_map={},
    )

    assert len(result) == 1
    assert result[0]["aid"] == "12345678"


# ------------------------------------------------------- _should_reprice ----

@pytest.mark.parametrize(
    ("current", "target", "age", "expected"),
    [
        (20.0, 18.0, 100.0, True),    # 10% drop
        (20.0, 19.5, 1.0, False),     # small drop, fresh
        (20.0, 22.0, 1.0, False),     # rise but fresh
        (20.0, 20.1, 100.0, False),   # stale but <1% change
        (20.0, 22.0, 100.0, True),    # stale + 10% higher
    ],
)
def test_should_reprice_rules(current, target, age, expected):
    from app.sell_pipeline import _should_reprice

    reason = _should_reprice(current, target, age, 5, 72, 6)
    assert bool(reason) is expected, reason


def test_should_reprice_cooldown_only_for_repeat_repricing():
    from app.sell_pipeline import _should_reprice

    # 首次挂单（kind="list"）即使刚上架也允许立刻纠偏
    assert _should_reprice(20.0, 10.0, 1.0, 5, 72, 6, "list") is not None
    # 刚重挂过的挂单在冷却期内不再重复重挂
    assert _should_reprice(20.0, 10.0, 1.0, 5, 72, 6, "reprice") is None
    assert _should_reprice(20.0, 10.0, 7.0, 5, 72, 6, "reprice") is not None


# --------------------------------------------------- _build_repricing_plan --

def _repricing_args(**overrides):
    args = dict(
        ctx=_build_ctx([]),
        cfg={"pipeline": {}},
        session=MagicMock(),
        pipeline_cfg={},
        purchases_snapshot=[
            {"assetid": "111", "name": "AK-47 | Redline", "_db_id": 7},
        ],
        sales_snapshot=[
            {"assetid": "111", "name": "AK-47 | Redline", "price": 20.0, "at": 1_000_000.0},
        ],
        active_listing_ids={"111"},
        listing_assetid_to_name={"111": "AK-47 | Redline"},
        account_currency="CNY",
        rate_map={},
    )
    args.update(overrides)
    return args


def _patch_price(monkeypatch, value):
    from app import sell_pipeline
    monkeypatch.setattr(sell_pipeline, "get_sell_orders_cny", lambda *_a, **_k: _orders())
    monkeypatch.setattr(
        sell_pipeline, "compute_smart_list_price", lambda *_a, **_k: (value, "wall")
    )


def test_build_repricing_plan_triggers_on_price_drop(monkeypatch):
    from app.sell_pipeline import _build_repricing_plan

    _patch_price(monkeypatch, 10.0)
    monkeypatch.setattr("app.sell_pipeline.time.time", lambda: 1_400_000.0)

    plan = _build_repricing_plan(**_repricing_args())

    assert len(plan) == 1
    assert plan[0]["aid"] == "111"
    assert plan[0]["list_price"] == 10.0
    assert plan[0]["old_price"] == 20.0
    assert "新价低于现价" in plan[0]["reason"]
    assert plan[0]["price_cents"] > 0


def test_build_repricing_plan_skips_without_local_record(monkeypatch):
    from app.sell_pipeline import _build_repricing_plan

    _patch_price(monkeypatch, 10.0)
    plan = _build_repricing_plan(**_repricing_args(sales_snapshot=[]))
    assert plan == []


def test_build_repricing_plan_skips_unowned_listing(monkeypatch):
    from app.sell_pipeline import _build_repricing_plan

    _patch_price(monkeypatch, 10.0)
    plan = _build_repricing_plan(**_repricing_args(purchases_snapshot=[]))
    assert plan == []


def test_build_repricing_plan_disabled(monkeypatch):
    from app.sell_pipeline import _build_repricing_plan

    _patch_price(monkeypatch, 10.0)
    plan = _build_repricing_plan(
        **_repricing_args(pipeline_cfg={"sell_reprice_enabled": False})
    )
    assert plan == []


def test_build_repricing_plan_fixes_fresh_mispriced_listing(monkeypatch):
    """首次挂单价格明显偏高时应立即重挂，而不是等冷却结束。"""
    from app.sell_pipeline import _build_repricing_plan

    _patch_price(monkeypatch, 10.0)
    monkeypatch.setattr("app.sell_pipeline.time.time", lambda: 1_000_600.0)

    plan = _build_repricing_plan(
        **_repricing_args(sales_snapshot=[
            {"assetid": "111", "name": "AK-47 | Redline", "price": 20.0,
             "at": 1_000_000.0, "kind": "list"},
        ])
    )
    assert len(plan) == 1


def test_build_repricing_plan_respects_reprice_cooldown(monkeypatch):
    from app.sell_pipeline import _build_repricing_plan

    _patch_price(monkeypatch, 10.0)
    monkeypatch.setattr("app.sell_pipeline.time.time", lambda: 1_000_600.0)

    plan = _build_repricing_plan(
        **_repricing_args(sales_snapshot=[
            {"assetid": "111", "name": "AK-47 | Redline", "price": 20.0,
             "at": 1_000_000.0, "kind": "reprice"},
        ])
    )
    assert plan == []


def test_build_repricing_plan_keeps_fresh_listing(monkeypatch):
    from app.sell_pipeline import _build_repricing_plan

    _patch_price(monkeypatch, 19.5)
    monkeypatch.setattr("app.sell_pipeline.time.time", lambda: 1_000_360.0)
    plan = _build_repricing_plan(**_repricing_args())
    assert plan == []


def test_build_repricing_plan_requires_rate_for_foreign_currency(monkeypatch):
    from app.sell_pipeline import _build_repricing_plan

    _patch_price(monkeypatch, 10.0)
    logged = []
    args = _repricing_args(account_currency="INR", rate_map={})
    args["ctx"] = _build_ctx(logged)
    plan = _build_repricing_plan(**args)
    assert plan == []
    assert any(level == "error" and "缺少汇率" in msg for level, msg in logged)


# ------------------------------------------------------- _reprice_listings --

def test_reprice_listings_delists_and_relists(monkeypatch):
    from app import sell_pipeline

    ctx = _build_ctx([])
    ctx.state.get_purchases.return_value = [{"assetid": "111", "_db_id": 7}]
    recorded = []

    monkeypatch.setattr(
        sell_pipeline, "delist_item", lambda aid, name, log_fn=None: (True, "999", None)
    )
    monkeypatch.setattr(
        sell_pipeline,
        "list_item",
        lambda *_a, **_k: {"status_code": 200, "text": json.dumps({"success": True})},
    )
    monkeypatch.setattr(sell_pipeline, "jittered_sleep", lambda *_a: None)
    monkeypatch.setattr(sell_pipeline, "append_sale", lambda row: recorded.append(row))
    monkeypatch.setattr(sell_pipeline._listing_cooldown, "remaining", lambda: 0)

    entry = {
        "aid": "111",
        "name": "AK-47 | Redline",
        "market_hash_name": "AK-47 | Redline",
        "appid": 730,
        "contextid": "2",
        "list_price": 10.0,
        "old_price": 20.0,
        "age_hours": 100.0,
        "price_cents": 900,
        "reason": "新价低于现价 50.0%",
    }
    repriced = sell_pipeline._reprice_listings(ctx, [entry], MagicMock(), "sid", 0)

    assert repriced == 1
    ctx.state.update_purchase_by_id.assert_any_call(
        7, {"listing": False, "listing_status": None, "assetid": "999"}
    )
    assert recorded and recorded[0]["assetid"] == "999"
    assert recorded[0]["price"] == 10.0
    assert recorded[0]["kind"] == "reprice"


def test_reprice_listings_aborts_when_delist_fails(monkeypatch):
    from app import sell_pipeline

    ctx = _build_ctx([])
    ctx.state.get_purchases.return_value = [{"assetid": "111", "_db_id": 7}]
    monkeypatch.setattr(
        sell_pipeline, "delist_item", lambda aid, name, log_fn=None: (False, None, "boom")
    )
    monkeypatch.setattr(sell_pipeline, "jittered_sleep", lambda *_a: None)
    monkeypatch.setattr(sell_pipeline._listing_cooldown, "remaining", lambda: 0)

    entry = {
        "aid": "111", "name": "AK-47 | Redline", "market_hash_name": "AK-47 | Redline",
        "appid": 730, "contextid": "2", "list_price": 10.0, "old_price": 20.0,
        "age_hours": 100.0, "price_cents": 900, "reason": "x",
    }
    repriced = sell_pipeline._reprice_listings(ctx, [entry], MagicMock(), "sid", 0)

    assert repriced == 0
    ctx.state.update_purchase_by_id.assert_not_called()


def test_reprice_listings_rebinds_assetid_without_relisting(monkeypatch):
    from app import sell_pipeline

    ctx = _build_ctx([])
    ctx.state.get_purchases.return_value = [{"assetid": "111", "_db_id": 7}]
    listed = []
    monkeypatch.setattr(
        sell_pipeline, "delist_item", lambda aid, name, log_fn=None: (True, None, None)
    )
    monkeypatch.setattr(sell_pipeline, "list_item", lambda *_a, **_k: listed.append(1))
    monkeypatch.setattr(sell_pipeline, "jittered_sleep", lambda *_a: None)
    monkeypatch.setattr(sell_pipeline._listing_cooldown, "remaining", lambda: 0)

    entry = {
        "aid": "111", "name": "AK-47 | Redline", "market_hash_name": "AK-47 | Redline",
        "appid": 730, "contextid": "2", "list_price": 10.0, "old_price": 20.0,
        "age_hours": 100.0, "price_cents": 900, "reason": "x",
    }
    repriced = sell_pipeline._reprice_listings(ctx, [entry], MagicMock(), "sid", 0)

    assert repriced == 0
    assert listed == []
    ctx.state.update_purchase_by_id.assert_any_call(
        7, {"assetid": None, "listing": False, "listing_status": None}
    )


# -------------------------------------------------------- _run_reprice_phase -

def test_run_reprice_phase_skipped_without_listings():
    from app.sell_pipeline import _run_reprice_phase

    ctx = _build_ctx([])
    assert _run_reprice_phase(
        ctx, {"pipeline": {}}, MagicMock(), "sid", {}, [], False, set(), {},
        "CNY", {}, 0,
    ) == 0
    assert _run_reprice_phase(
        ctx, {"pipeline": {}}, MagicMock(), "sid", {}, [], True, set(), {},
        "CNY", {}, 0,
    ) == 0


def test_run_reprice_phase_warns_when_confirm_disabled(monkeypatch):
    from app import sell_pipeline
    from app.sell_pipeline import _run_reprice_phase

    logged = []
    ctx = _build_ctx(logged)
    ctx.state.get_sales.return_value = [
        {"assetid": "111", "name": "AK-47 | Redline", "price": 20.0, "at": 1_000_000.0}
    ]
    monkeypatch.setattr(sell_pipeline, "_build_repricing_plan", lambda *a, **k: [{"aid": "111"}])
    monkeypatch.setattr(sell_pipeline, "_reprice_listings", lambda *a, **k: 1)

    repriced = _run_reprice_phase(
        ctx, {"pipeline": {}, "steam_confirm": {"enabled": False}},
        MagicMock(), "sid", {}, [
            {"assetid": "111", "name": "AK-47 | Redline"},
        ], True, {"111"}, {"111": "AK-47 | Redline"}, "CNY", {}, 0,
    )

    assert repriced == 1
    assert any("未开启自动确认" in msg for _, msg in logged)
