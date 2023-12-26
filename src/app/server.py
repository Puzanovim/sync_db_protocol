import asyncio
from uuid import uuid4, UUID
from multiprocessing import Process

import orjson

from schemas import Address
import logging

from src.app.commands import ANSWER_COMMAND
from src.app.repositories.nodes_repo import NodeRepository

logger = logging.getLogger(__name__)


class Server(Process):
    def __init__(self, name: str, address: Address):
        super().__init__()
        self.name = name
        self.address: Address = address
        self.uuid: UUID = uuid4()
        self.__timeout: int = 10

        self.nodes_db = NodeRepository(f'db_data/{self.name}_{self.uuid}.db')
        self.nodes_db.init_db()

    def info_nodes(self) -> dict:
        return {'name': self.name, 'uuid': self.uuid, 'nodes': self.nodes_db.get_nodes()}

    async def __send_receive(self, message: dict, node_address: Address) -> dict:
        reader, writer = await asyncio.open_connection(node_address.address, node_address.port)

        logger.warning(f'{self.name}:Send.     {message['COMMAND']}.\tTo {node_address}.\t\t\t\t\t\t\t({message})')
        writer.write(orjson.dumps(message))

        try:
            data = await asyncio.wait_for(reader.read(1000), self.__timeout)
        except asyncio.TimeoutError:
            logger.warning(f'{self.name}. Timeout from {node_address}')
            raise TimeoutError
        else:
            message = orjson.loads(data)
            logger.warning(f'{self.name}:Received. {message['COMMAND']}.\tFrom {node_address}.\t\t\t\t\t\t\t({message})')
            return message
        finally:
            writer.close()

    async def add_node(self, new_node_address: Address):
        message: dict = {
            'UUID': self.uuid,
            'UUID_NEW_NODE': None,
            'COMMAND': 'REQUEST_JOIN',
            'data': None,
        }
        try:
            message = await self.__send_receive(message, new_node_address)
        except TimeoutError:
            return None

        new_node_id = message.get('UUID')
        nodes = self.nodes_db.get_nodes()

        message: dict = {
            'UUID': self.uuid,
            'UUID_NEW_NODE': new_node_id,
            'COMMAND': 'UPDATE_JOIN',
            'data': {'node_address': self.address.model_dump_json()},
        }
        if nodes:
            message['data']['nodes'] = {node: address.model_dump_json() for node, address in nodes.items()}
        try:
            await self.__send_receive(message, new_node_address)
        except TimeoutError:
            return None

        message: dict = {
            'UUID': self.uuid,
            'UUID_NEW_NODE': new_node_id,
            'COMMAND': 'JOIN_NODE',
            'data': {'node_address': new_node_address.model_dump_json()},
        }
        for node_id, node_address in nodes.items():
            try:
                message = await self.__send_receive(message, node_address)
            except TimeoutError:
                continue

        self.nodes_db.insert_node(new_node_id, new_node_address)

    async def commit_transaction(self, transaction: str):
        transaction_id: UUID = uuid4()

        # reader, writer = await asyncio.open_connection(node_address.address, node_address.port)
        #
        # message: dict = {
        #     'UUID_NODE': self.uuid,
        #     'UUID_NEW_NODE': transaction_id,
        #     'COMMAND': 'REQUEST_JOIN',
        #     'data': None,
        # }
        #
        # print(f'Send: {message!r}')
        # writer.write(orjson.dumps(message))
        #
        # data = await reader.read(100)
        # print(f'Received: {data.decode()!r}')
        #
        # print('Close the connection')
        # writer.close()

    async def handle_commands(self, reader, writer):
        data = await reader.read(1000)
        message = orjson.loads(data)
        addr = writer.get_extra_info('peername')
        logger.warning(f'\t\t\t\t{self.name}. Received\t{message['COMMAND']}.\tFrom {addr}.\t\t({message})')

        command = message.get('COMMAND')
        answer_command = ANSWER_COMMAND.get(command, 'UNKNOWN')
        answer_data = None

        match command:
            case 'UPDATE_JOIN':
                node_id = message.get('UUID')
                message_data = message.get('data')
                node_address = Address.model_validate(orjson.loads(message_data.get('node_address')))
                self.nodes_db.insert_node(node_id, node_address)

                nodes = message_data.get('nodes', {})
                for uuid, address in nodes.items():
                    self.nodes_db.insert_node(uuid, Address.model_validate(orjson.loads(address)))

                answer_data = None
            case 'JOIN_NODE':
                node_id = message.get('UUID_NEW_NODE')
                node_address = Address.model_validate(orjson.loads(message.get('data').get('node_address')))
                self.nodes_db.insert_node(node_id, node_address)
                answer_data = None

        message: dict = {
            **message,
            'UUID': self.uuid,
            'COMMAND': answer_command,
            'data': answer_data,
        }
        logger.warning(f'\t\t\t\t{self.name}. Send\t\t{message['COMMAND']}.\tTo {addr}.\t\t({message})')
        writer.write(orjson.dumps(message))
        await writer.drain()
        writer.close()

    async def start_server(self) -> None:
        server = await asyncio.start_server(self.handle_commands, self.address.address, self.address.port)

        addr = server.sockets[0].getsockname()
        print(f'Serving on {addr}')

        async with server:
            await server.serve_forever()

    def run(self):
        logger.warning(f'Starting server with id: {self.uuid} address: {self.address}')
        asyncio.run(self.start_server())
