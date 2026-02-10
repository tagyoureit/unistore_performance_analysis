import pytest
from starlette.requests import Request
from starlette.responses import RedirectResponse


def _make_request(path: str, *, hx: bool = False, query_string: str = "") -> Request:
    headers = []
    if hx:
        headers.append((b"hx-request", b"true"))

    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode("utf-8"),
        "query_string": query_string.encode("utf-8"),
        "root_path": "",
        "headers": headers,
        "client": ("testclient", 123),
        "server": ("testserver", 80),
    }
    return Request(scope)


@pytest.mark.asyncio
async def test_dashboard_test_renders_live_for_prepared(monkeypatch):
    from backend import main as main_app

    test_id = "test-prepared"

    class StubRunning:
        status = "PREPARED"

    async def fake_get(_: str):
        return StubRunning()

    monkeypatch.setattr(main_app.registry, "get", fake_get)

    req = _make_request(f"/dashboard/{test_id}")
    resp = await main_app.dashboard_test(req, test_id)

    assert not isinstance(resp, RedirectResponse)
    assert resp.context.get("test_id") == test_id
    assert resp.template.name == "pages/dashboard.html"


@pytest.mark.asyncio
async def test_dashboard_test_redirects_for_terminal_status(monkeypatch):
    from backend import main as main_app

    test_id = "test-completed"

    class StubRunning:
        status = "COMPLETED"

    async def fake_get(_: str):
        return StubRunning()

    monkeypatch.setattr(main_app.registry, "get", fake_get)

    req = _make_request(f"/dashboard/{test_id}")
    resp = await main_app.dashboard_test(req, test_id)

    assert isinstance(resp, RedirectResponse)
    assert resp.status_code == 302
    assert resp.headers.get("location") == f"/dashboard/history/{test_id}"


@pytest.mark.asyncio
async def test_dashboard_test_redirects_when_not_in_registry(monkeypatch):
    from backend import main as main_app

    test_id = "test-missing"

    async def fake_get(_: str):
        return None

    monkeypatch.setattr(main_app.registry, "get", fake_get)

    req = _make_request(f"/dashboard/{test_id}")
    resp = await main_app.dashboard_test(req, test_id)

    assert isinstance(resp, RedirectResponse)
    assert resp.status_code == 302
    assert resp.headers.get("location") == f"/dashboard/history/{test_id}"


@pytest.mark.asyncio
async def test_dashboard_test_prefers_run_status_over_stale_parent_row(monkeypatch):
    from backend import main as main_app

    test_id = "test-stale-parent"

    async def fake_get(_: str):
        return None

    class StubPool:
        async def execute_query(self, query: str, params=None):
            # TEST_RESULTS status is stale RUNNING, but RUN_STATUS is terminal.
            return [("RUNNING", "COMPLETED")]

    monkeypatch.setattr(main_app.registry, "get", fake_get)
    monkeypatch.setattr(
        main_app.snowflake_pool, "get_default_pool", lambda: StubPool()
    )

    req = _make_request(f"/dashboard/{test_id}")
    resp = await main_app.dashboard_test(req, test_id)

    assert isinstance(resp, RedirectResponse)
    assert resp.status_code == 302
    assert resp.headers.get("location") == f"/dashboard/history/{test_id}"


@pytest.mark.asyncio
async def test_dashboard_test_htmx_terminal_sets_hx_redirect(monkeypatch):
    from backend import main as main_app

    test_id = "test-completed-htmx"

    class StubRunning:
        status = "FAILED"

    async def fake_get(_: str):
        return StubRunning()

    monkeypatch.setattr(main_app.registry, "get", fake_get)

    req = _make_request(f"/dashboard/{test_id}", hx=True)
    resp = await main_app.dashboard_test(req, test_id)

    assert not isinstance(resp, RedirectResponse)
    assert resp.status_code == 200
    assert resp.headers.get("HX-Redirect") == f"/dashboard/history/{test_id}"


# -----------------------------------------------------------------------------
# Section 2.13: Legacy Removal - /comparison redirect tests
# -----------------------------------------------------------------------------


class TestComparisonDeprecatedRedirect:
    """Tests for deprecated /comparison route redirect to /history."""

    @pytest.mark.asyncio
    async def test_comparison_redirects_to_history(self):
        """Deprecated /comparison route redirects to /history."""
        from backend import main as main_app

        req = _make_request("/comparison")
        resp = await main_app.comparison(req)

        assert isinstance(resp, RedirectResponse)
        assert resp.status_code == 303
        assert resp.headers.get("location") == "/history"

    @pytest.mark.asyncio
    async def test_comparison_htmx_uses_hx_redirect(self):
        """Deprecated /comparison with HTMX uses HX-Redirect header."""
        from backend import main as main_app

        req = _make_request("/comparison", hx=True)
        resp = await main_app.comparison(req)

        assert not isinstance(resp, RedirectResponse)
        assert resp.status_code == 200
        assert resp.headers.get("HX-Redirect") == "/history"


class TestHistoryCompareRoute:
    """Tests for /history/compare route validation."""

    @pytest.mark.asyncio
    async def test_history_compare_requires_exactly_two_ids(self):
        """/history/compare shows error when not exactly 2 IDs provided."""
        from backend import main as main_app

        # No IDs
        req = _make_request("/history/compare")
        resp = await main_app.history_compare(req)
        assert (
            resp.context.get("error")
            == "Provide exactly 2 test ids via ?ids=<id1>,<id2>."
        )

        # One ID
        req = _make_request("/history/compare", query_string="ids=test-1")
        resp = await main_app.history_compare(req)
        assert (
            resp.context.get("error")
            == "Provide exactly 2 test ids via ?ids=<id1>,<id2>."
        )

        # Three IDs
        req = _make_request("/history/compare", query_string="ids=test-1,test-2,test-3")
        resp = await main_app.history_compare(req)
        assert (
            resp.context.get("error")
            == "Provide exactly 2 test ids via ?ids=<id1>,<id2>."
        )

    @pytest.mark.asyncio
    async def test_history_compare_accepts_two_ids(self):
        """/history/compare accepts exactly 2 IDs without error."""
        from backend import main as main_app

        req = _make_request("/history/compare", query_string="ids=test-1,test-2")
        resp = await main_app.history_compare(req)

        assert resp.context.get("error") is None
        assert resp.context.get("ids") == ["test-1", "test-2"]
        assert resp.template.name == "pages/history_compare.html"

    @pytest.mark.asyncio
    async def test_history_compare_trims_whitespace(self):
        """/history/compare trims whitespace from IDs."""
        from backend import main as main_app

        req = _make_request("/history/compare", query_string="ids= test-1 , test-2 ")
        resp = await main_app.history_compare(req)

        assert resp.context.get("error") is None
        assert resp.context.get("ids") == ["test-1", "test-2"]

    @pytest.mark.asyncio
    async def test_history_compare_ignores_empty_segments(self):
        """/history/compare ignores empty segments in IDs."""
        from backend import main as main_app

        req = _make_request("/history/compare", query_string="ids=test-1,,test-2")
        resp = await main_app.history_compare(req)

        assert resp.context.get("error") is None
        assert resp.context.get("ids") == ["test-1", "test-2"]
