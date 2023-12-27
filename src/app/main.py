import asyncio

from src.app.server import Server
from schemas import Address


TRANSACTION_EXAMPLE = """
UPDATE accounts SET balance = balance - 100.00
    WHERE name = 'Alice';
UPDATE branches SET balance = balance - 100.00
    WHERE name = (SELECT branch_name FROM accounts WHERE name = 'Alice');
UPDATE accounts SET balance = balance + 100.00
    WHERE name = 'Bob';
"""


async def main():
    node1 = Server(name='node1', address=Address(address='localhost', port=15001))
    node2 = Server(name='node2', address=Address(address='localhost', port=15002))
    node3 = Server(name='node3', address=Address(address='localhost', port=15003))
    node4 = Server(name='node4', address=Address(address='localhost', port=15004))
    node5 = Server(name='node5', address=Address(address='localhost', port=15005))

    nodes: list[Server] = [node1, node2, node3, node4, node5]
    for node in nodes:
        node.start()

    print('Подключение node2 к node1')
    await node1.add_node(node2.address)
    print('Подключение node3 к когорте')
    await node1.add_node(node3.address)
    await asyncio.sleep(0.1)
    for node in nodes:
        print(node.info_nodes())

    print('Произошло принятие транзакции на узле node2')
    await node2.commit_transaction(TRANSACTION_EXAMPLE)
    await asyncio.sleep(0.1)
    print('Результаты синхронизации транзакции')
    for node in nodes:
        print(node.info_nodes())

    print('Подключение node4 к когорте')
    await node1.add_node(node4.address)
    await asyncio.sleep(0.1)
    for node in nodes:
        print(node.info_nodes())
    print('Произошло принятие транзакции на узле node2. На узле node4 на второй фазе произошел отказ')
    await node2.commit_transaction(TRANSACTION_EXAMPLE)
    await asyncio.sleep(0.1)
    print('Результаты синхронизации транзакции')
    for node in nodes:
        print(node.info_nodes())
    nodes.pop(3)

    print('Подключение node5 к когорте')
    await node1.add_node(node5.address)
    await asyncio.sleep(0.1)
    for node in nodes:
        print(node.info_nodes())
    print('Произошло принятие транзакции на узле node2. На узле node5 на первой фазе произошел конфликт')
    await node2.commit_transaction(TRANSACTION_EXAMPLE)
    await asyncio.sleep(0.1)
    print('Результаты синхронизации транзакции')
    for node in nodes:
        print(node.info_nodes())

    print('Повторное подключение node4 к когорте')
    node4 = Server(name='node4', address=Address(address='localhost', port=15004))
    node4.start()
    await node1.add_node(node4.address)
    nodes.insert(3, node4)
    for node in nodes:
        print(node.info_nodes())


if __name__ == '__main__':
    asyncio.run(main())
