from fastapi.testclient import TestClient

from backend.src.main import app


class _DummyProcessor:
    def process_query(self, q: str):
        return {
            "answer": f"echo: {q}",
            "sources": [],
            "data": {"echo": q},
            "metadata": {},
        }


def setup_module(module):
    # Replace real processor with dummy to avoid external calls in tests
    app.state.processor = _DummyProcessor()


def test_query_empty_returns_400():
    client = TestClient(app)
    res = client.post("/api/query", json={"query": ""})
    assert res.status_code == 400
    body = res.json()
    assert "detail" in body


def test_query_ok():
    client = TestClient(app)
    res = client.post("/api/query", json={"query": "hello"})
    assert res.status_code == 200
    body = res.json()
    assert body["answer"].startswith("echo: ")
    assert "sources" in body
    assert "data" in body
    assert "metadata" in body


