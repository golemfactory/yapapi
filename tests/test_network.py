import sys
from unittest import mock

import pytest
from statemachine.exceptions import TransitionNotAllowed

from yapapi.network import Network, NetworkError, NetworkState

if sys.version_info >= (3, 8):
    from tests.factories.network import NetworkFactory


@pytest.mark.skipif(sys.version_info < (3, 8), reason="AsyncMock requires python 3.8+")
class TestNetwork:
    def test_init(self):
        ip = "192.168.0.0"
        network = Network(mock.Mock(), f"{ip}/24", "0xdeadbeef")
        assert network.owner_ip == "192.168.0.1"
        assert network.network_address == ip
        assert network.netmask == "255.255.255.0"

    def test_init_mask(self):
        ip = "192.168.0.0"
        mask = "255.255.0.0"
        network = Network(mock.Mock(), ip, "0xcafed00d", mask=mask)
        assert network.network_address == ip
        assert network.netmask == mask

    def test_init_duplicate_mask(self):
        with pytest.raises(NetworkError):
            Network(mock.Mock(), "10.0.0.0/16", "0x0d15ea5e", mask="255.255.0.0")

    @pytest.mark.asyncio
    def test_create(self):
        ip = "192.168.0.0"
        owner_id = "0xcafebabe"
        network = NetworkFactory(ip=f"{ip}/24", owner_id=owner_id)
        assert network.network_id
        assert network.owner_ip == "192.168.0.1"
        assert network.network_address == ip
        assert network.netmask == "255.255.255.0"
        assert network.nodes_dict == {"192.168.0.1": owner_id}
        assert network.state == NetworkState.ready
        network._net_api.create_network.assert_called_with(
            network.network_address, network.netmask, network.gateway
        )

    @pytest.mark.asyncio
    def test_create_with_owner_ip(self):
        network = NetworkFactory(ip="192.168.0.0/24", owner_ip="192.168.0.2")
        assert list(network.nodes_dict.keys()) == ["192.168.0.2"]

    @pytest.mark.asyncio
    def test_create_with_owner_ip_outside_network(self):
        with pytest.raises(NetworkError) as e:
            NetworkFactory(ip="192.168.0.0/24", owner_ip="192.168.1.1")

        assert "address must belong to the network" in str(e.value)

    @pytest.mark.asyncio
    async def test_add_node(self):
        network = NetworkFactory(ip="192.168.0.0/24")
        node1 = await network.add_node("1")
        assert node1.ip == "192.168.0.2"
        node2 = await network.add_node("2")
        assert node2.ip == "192.168.0.3"

    @pytest.mark.asyncio
    async def test_add_node_owner_ip_different(self):
        network = NetworkFactory(ip="192.168.0.0/24", owner_ip="192.168.0.2")
        node1 = await network.add_node("1")
        assert node1.ip == "192.168.0.1"
        node2 = await network.add_node("2")
        assert node2.ip == "192.168.0.3"

    @pytest.mark.asyncio
    async def test_add_node_specific_ip(self):
        network = NetworkFactory(ip="192.168.0.0/24")
        ip = "192.168.0.5"
        node = await network.add_node("1", ip)
        assert node.ip == ip

    @pytest.mark.asyncio
    async def test_add_node_ip_collision(self):
        network = NetworkFactory(ip="192.168.0.0/24", owner_ip="192.168.0.2")
        with pytest.raises(NetworkError) as e:
            await network.add_node("1", "192.168.0.2")

        assert "has already been assigned in this network" in str(e.value)

    @pytest.mark.asyncio
    async def test_add_node_ip_outside_network(self):
        network = NetworkFactory(ip="192.168.0.0/24")
        with pytest.raises(NetworkError) as e:
            await network.add_node("1", "192.168.1.2")

        assert "address must belong to the network" in str(e.value)

    @pytest.mark.asyncio
    async def test_add_node_pool_depleted(self):
        network = NetworkFactory(ip="192.168.0.0/30")
        await network.add_node("1")
        with pytest.raises(NetworkError) as e:
            await network.add_node("2")

        assert "No more addresses available" in str(e.value)

    @pytest.mark.asyncio
    async def test_id_when_initialized(self):
        network = Network(mock.Mock(), f"192.168.0.0/24", "0xdeadbeef")
        with pytest.raises(TransitionNotAllowed, match=".*Can't get_id when in initialized.*") as e:
            im_gonna_fail = network.network_id

    @pytest.mark.asyncio
    async def test_id_when_removed(self):
        network = NetworkFactory(ip="192.168.0.0/24")
        assert network.network_id

        await network.remove()

        with pytest.raises(TransitionNotAllowed, match=".*Can't get_id when in removed.*") as e:
            im_gonna_fail = network.network_id

    @pytest.mark.asyncio
    async def test_remove(self):
        network = NetworkFactory(ip="192.168.0.0/24")

        await network.remove()

        network._net_api.remove_network.assert_called_once()

    @pytest.mark.asyncio
    async def test_remove_when_initialized(self):
        network = Network(mock.Mock(), f"192.168.0.0/24", "0xdeadbeef")
        with pytest.raises(TransitionNotAllowed, match=".*Can't stop when in initialized.*") as e:
            await network.remove()

    @pytest.mark.asyncio
    async def test_remove_when_removed(self):
        network = NetworkFactory(ip="192.168.0.0/24")

        await network.remove()

        with pytest.raises(TransitionNotAllowed, match=".*Can't stop when in removed.*") as e:
            await network.remove()

    @pytest.mark.asyncio
    async def test_network_context_manager(self):
        network = NetworkFactory(ip="192.168.0.0/24")
        assert network.state == NetworkState.ready

        async with network:
            pass

        assert network.state == NetworkState.removed
