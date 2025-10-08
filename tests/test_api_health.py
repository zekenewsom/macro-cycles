from fastapi.testclient import TestClient

from apps.api.main import app


def test_health_endpoint_returns_ok():
    with TestClient(app) as client:
        r = client.get("/health")
        assert r.status_code == 200
        body = r.json()
        assert body.get("status") == "ok"
        assert "ts" in body

