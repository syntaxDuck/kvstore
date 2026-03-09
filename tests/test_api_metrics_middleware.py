import pytest
from starlette.requests import Request
from starlette.responses import Response

from src.api.main import MetricsMiddleware
from src.core.metrics import get_metrics


@pytest.mark.asyncio
async def test_metrics_middleware_uses_route_template_not_raw_path():
    metrics = get_metrics()
    metrics.reset()

    middleware = MetricsMiddleware(app=lambda _scope, _receive, _send: None)
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/client/v1/kv/alpha",
        "headers": [],
        "query_string": b"",
        "scheme": "http",
        "server": ("test", 80),
        "client": ("127.0.0.1", 12345),
        "root_path": "",
        "route": type("Route", (), {"path": "/client/v1/kv/{key}"})(),
    }
    request = Request(scope)

    async def call_next(_request: Request) -> Response:
        return Response(status_code=200)

    await middleware.dispatch(request, call_next)

    all_metrics = metrics.get_all_metrics()
    templated_key = "api_request_count.GET /client/v1/kv/{key}.200"
    raw_key = "api_request_count.GET /client/v1/kv/alpha.200"
    assert templated_key in all_metrics
    assert raw_key not in all_metrics
