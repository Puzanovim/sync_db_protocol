import asyncio

from src.app.server import Server
from schemas import Address


async def main():
    node1 = Server(name='node1', address=Address(address='localhost', port=15001))
    node2 = Server(name='node2', address=Address(address='localhost', port=15002))
    node3 = Server(name='node3', address=Address(address='localhost', port=15003))

    nodes: list[Server] = [node1, node2, node3]

    for node in nodes:
        node.start()

    await node1.add_node(node2.address)
    await node1.add_node(node3.address)

    for node in nodes:
        print(node.info_nodes())


if __name__ == '__main__':
    asyncio.run(main())
