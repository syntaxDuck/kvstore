import pytest

from src.core.rpc import RpcRequest, RpcResponse, RpcDipatcher
from src.core.types import Command


class TestRpcRequest:
    def test_ping_factory(self):
        req = RpcRequest.ping()
        assert req.type == "PING"
        assert req.cmd is None

    def test_client_write_factory(self):
        cmd = Command(op="SET", key="foo", val="bar")
        req = RpcRequest.client_write(cmd)
        assert req.type == "CLIENT_WRITE"
        assert req.cmd.op == "SET"
        assert req.cmd.key == "foo"

    def test_append_entry_factory(self):
        cmd = Command(op="SET", key="foo", val="bar")
        req = RpcRequest.append_entry(cmd)
        assert req.type == "APPEND_ENTRY"
        assert req.cmd.op == "SET"

    def test_with_cmd(self):
        cmd = Command(op="GET", key="test", val=None)
        req = RpcRequest(type="CUSTOM", cmd=cmd)
        assert req.type == "CUSTOM"
        assert req.cmd.op == "GET"
        assert req.cmd.key == "test"


class TestRpcResponse:
    def test_ok_factory(self):
        res = RpcResponse.ok(1)
        assert res.status == "OK"
        assert res.node_id == 1
        assert res.payload == {}
        assert res.is_ok is True

    def test_ok_factory_with_payload(self):
        res = RpcResponse.ok(1, {"data": "value"})
        assert res.status == "OK"
        assert res.node_id == 1
        assert res.payload == {"data": "value"}

    def test_ack_factory(self):
        res = RpcResponse.ack(1)
        assert res.status == "ACK"
        assert res.node_id == 1
        assert res.payload is None

    def test_err_factory(self):
        res = RpcResponse.err()
        assert res.status == "ERROR"
        assert res.node_id is None
        assert res.payload == {}
        assert res.is_ok is False

    def test_err_factory_with_node_and_payload(self):
        res = RpcResponse.err(1, {"error": "message"})
        assert res.status == "ERROR"
        assert res.node_id == 1
        assert res.payload == {"error": "message"}
        assert res.is_ok is False

    def test_is_ok_property_true(self):
        res = RpcResponse(status="OK", node_id=1, payload={})
        assert res.is_ok is True

    def test_is_ok_property_false(self):
        res = RpcResponse(status="ACK", node_id=1, payload={})
        assert res.is_ok is False

    def test_is_ok_property_error(self):
        res = RpcResponse(status="ERROR", node_id=1, payload={})
        assert res.is_ok is False


class TestRpcDispatcher:
    @pytest.mark.asyncio
    async def test_register_and_dispatch(self):
        dispatcher = RpcDipatcher()

        async def handler(req):
            return RpcResponse.ok(42)

        dispatcher.register("TEST", handler)
        cmd = Command(op="SET", key="foo", val="bar")
        req = RpcRequest(type="TEST", cmd=cmd)
        res = await dispatcher.dispatch(req)
        assert res.is_ok is True
        assert res.node_id == 42

    @pytest.mark.asyncio
    async def test_dispatch_unknown_command_raises(self):
        dispatcher = RpcDipatcher()
        req = RpcRequest(type="UNKNOWN", cmd=None)
        with pytest.raises(ValueError, match="Unknown command"):
            await dispatcher.dispatch(req)

    @pytest.mark.asyncio
    async def test_dispatch_case_insensitive(self):
        dispatcher = RpcDipatcher()

        async def handler(req):
            return RpcResponse.ok(1)

        dispatcher.register("test", handler)
        req = RpcRequest(type="TEST", cmd=None)
        res = await dispatcher.dispatch(req)
        assert res.is_ok is True

    @pytest.mark.asyncio
    async def test_multiple_handlers(self):
        dispatcher = RpcDipatcher()

        async def handler1(req):
            return RpcResponse.ok(1)

        async def handler2(req):
            return RpcResponse.ok(2)

        dispatcher.register("CMD1", handler1)
        dispatcher.register("CMD2", handler2)

        req1 = RpcRequest(type="CMD1", cmd=None)
        req2 = RpcRequest(type="CMD2", cmd=None)

        res1 = await dispatcher.dispatch(req1)
        res2 = await dispatcher.dispatch(req2)

        assert res1.node_id == 1
        assert res2.node_id == 2
