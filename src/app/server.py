import asyncio
import os
from uuid import uuid4, UUID
from multiprocessing import Process

import orjson

from schemas import Address
import logging

from src.app.commands import ANSWER_COMMAND
from src.app.config import CustomFormatter
from src.app.repositories.nodes_repo import NodeRepository
from src.app.repositories.transactions_repo import TransactionsRepository


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(CustomFormatter())
logger.addHandler(ch)


class Server(Process):
    def __init__(self, name: str, address: Address):
        super().__init__()
        self.name = name
        self.address: Address = address
        self.uuid: UUID = uuid4()
        self.__timeout: int = 5
        self.__failure_count: int = 0

        self.__client_indent = '\t\t\t\t\t\t\t'
        self.__len_command_field = 15

        self.nodes_db = NodeRepository(f'db_data/nodes/{self.name}_{self.uuid}.db')
        self.nodes_db.init_db()

        self.transactions_db = TransactionsRepository(f'db_data/transactions/{self.name}_{self.uuid}.db')
        try:
            self.transactions_db.init_db()
        except Exception as exc:
            logger.exception(exc)

    def info_nodes(self) -> dict:
        return {
            'name': self.name,
            'uuid': self.uuid,
            'nodes': self.nodes_db.get_nodes(),
            'transactions': self.transactions_db.get_transactions(),
        }

    def server_log(self, command: str, addr: Address, msg: dict, received: bool) -> None:
        action = 'Received.' if received else 'Send.    '
        direction = 'From' if received else 'To  '
        command_field = f'{command}{' ' * (self.__len_command_field - len(command))}'
        logger.error(f'{self.name}:{action} {command_field}{direction} {addr}.{self.__client_indent}\t\t\t\t({msg})')

    def client_log(self, command: str, addr: str, msg: dict, received: bool) -> None:
        action = 'Received.' if received else 'Send.    '
        direction = 'From' if received else 'To  '
        command_field = f'{command}{' ' * (self.__len_command_field - len(command))}'
        logger.warning(f'{self.__client_indent}{self.name}:{action} {command_field}{direction} {addr}.\t\t\t({msg})')

    async def __send_receive(self, message: dict, node_address: Address) -> dict:
        reader, writer = await asyncio.open_connection(node_address.address, node_address.port)

        self.server_log(message['COMMAND'], node_address, message, received=False)
        writer.write(orjson.dumps(message))

        try:
            data = await asyncio.wait_for(reader.read(2000), self.__timeout)
        except (asyncio.TimeoutError, ConnectionResetError):
            logger.warning(f'{self.name}. Timeout from {node_address}')
            raise TimeoutError
        else:
            message = orjson.loads(data)
            self.server_log(message['COMMAND'], node_address, message, received=True)
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
        transactions = self.transactions_db.get_full_transactions()

        message: dict = {
            'UUID': self.uuid,
            'UUID_NEW_NODE': new_node_id,
            'COMMAND': 'UPDATE_JOIN',
            'data': {'node_address': self.address.model_dump_json()},
        }
        if nodes:
            message['data']['nodes'] = {node: address.model_dump_json() for node, address in nodes.items()}
        if transactions:
            message['data']['transactions'] = transactions
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
                await self.__send_receive(message, node_address)
            except TimeoutError:
                continue

        self.nodes_db.insert_node(new_node_id, new_node_address)

    async def abort_process(self, message: dict, nodes: list[Address]) -> None:
        message: dict = {
            **message,
            'COMMAND': 'ABORT',
        }
        for node_address in nodes:
            try:
                await self.__send_receive(message, node_address)
            except TimeoutError:
                continue

    async def delete_node(self, deleted_node_id: str, deleted_node_address: Address) -> None:
        message: dict = {
            'UUID': self.uuid,
            'UUID_NODE': deleted_node_id,
            'COMMAND': 'DELETE_NODE',
            'data': None,
        }
        try:
            await self.__send_receive(message, deleted_node_address)
        except (TimeoutError, ConnectionRefusedError):
            pass

        nodes = self.nodes_db.get_nodes()

        message: dict = {
            'UUID': self.uuid,
            'UUID_NODE': deleted_node_id,
            'COMMAND': 'DELETE_NODE',
            'data': None,
        }
        for node_id, node_address in nodes.items():
            if node_address.address == deleted_node_address.address and node_address.port == deleted_node_address.port:
                continue

            try:
                await self.__send_receive(message, node_address)
            except TimeoutError:
                pass

        self.nodes_db.delete_node(deleted_node_id)

    async def timeout_node_handler(self, node_id: str, node_address: Address) -> None:
        failure_count = self.nodes_db.get_node_failure_count(node_id)
        if failure_count == self.__failure_count:
            await self.delete_node(node_id, node_address)
        else:
            self.nodes_db.update_node_failure_count(node_id, failure_count + 1)

    async def commit_transaction(self, transaction: str):
        transaction_id: UUID = uuid4()
        nodes = self.nodes_db.get_nodes()

        self.transactions_db.insert_transaction(transaction_id, self.uuid, 'PREPARE', transaction)
        check_result, check_text = self.transactions_db.check_transaction(transaction)
        if not check_result:
            self.transactions_db.update_transaction_state(transaction_id, 'ABORTED')
            logger.warning(f'{self.name}. Transaction denied: {check_text}')
            return None

        message: dict = {
            'UUID': self.uuid,
            'GTID': transaction_id,
            'COMMAND': 'PREPARE',
            'data': {'transaction': transaction},
        }
        sent_nodes = []
        for node_id, node_address in nodes.items():
            try:
                answer_message = await self.__send_receive(message, node_address)
            except TimeoutError:
                self.transactions_db.update_transaction_state(transaction_id, 'ABORTED')
                await self.abort_process(message, sent_nodes)
                await self.timeout_node_handler(node_id, node_address)
                return None
            else:
                if answer_message['COMMAND'] == 'DENY':
                    self.transactions_db.update_transaction_state(transaction_id, 'ABORTED')
                    await self.abort_process(message, sent_nodes)
                    return None
                self.transactions_db.update_transaction_state(transaction_id, answer_message['COMMAND'])
                sent_nodes.append(node_address)

        self.transactions_db.update_transaction_state(transaction_id, 'PRECOMMIT')
        message: dict = {
            'UUID': self.uuid,
            'GTID': transaction_id,
            'COMMAND': 'PRECOMMIT',
            'data': {'transaction': transaction},
        }
        sent_nodes = []
        for node_id, node_address in nodes.items():
            try:
                answer_message = await self.__send_receive(message, node_address)
            except TimeoutError:
                self.transactions_db.update_transaction_state(transaction_id, 'ABORTED')
                await self.abort_process(message, sent_nodes)
                await self.timeout_node_handler(node_id, node_address)
                return None
            else:
                if answer_message['COMMAND'] == 'DENY':
                    self.transactions_db.update_transaction_state(transaction_id, 'ABORTED')
                    await self.abort_process(message, sent_nodes)
                    return None
                self.transactions_db.update_transaction_state(transaction_id, answer_message['COMMAND'])
                sent_nodes.append(node_address)

        self.transactions_db.save_to_binlog(transaction_id, transaction)

        self.transactions_db.update_transaction_state(transaction_id, 'COMMIT')
        message: dict = {
            'UUID': self.uuid,
            'GTID': transaction_id,
            'COMMAND': 'COMMIT',
            'data': None,
        }
        for node_id, node_address in nodes.items():
            try:
                await self.__send_receive(message, node_address)
            except TimeoutError:
                await self.timeout_node_handler(node_id, node_address)

        self.transactions_db.update_transaction_state(transaction_id, 'COMMITED')
        self.transactions_db.commit_transaction(transaction_id)
        logger.exception(f'{self.name}. Transaction commited.', exc_info=False)

    async def handle_commands(self, reader, writer):
        data = await reader.read(2000)
        message = orjson.loads(data)
        addr = writer.get_extra_info('peername')
        self.client_log(message['COMMAND'], addr, message, received=True)

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

                transactions = message_data.get('transactions', {})
                for uuid, (coordinator_id, state, transaction_text) in transactions.items():
                    self.transactions_db.insert_transaction(uuid, coordinator_id, state, transaction_text)

                answer_data = None
            case 'JOIN_NODE':
                node_id = message.get('UUID_NEW_NODE')
                node_address = Address.model_validate(orjson.loads(message.get('data').get('node_address')))
                self.nodes_db.insert_node(node_id, node_address)
                answer_data = None

            case 'DELETE_NODE':
                node_id = message.get('UUID_NODE')
                if node_id != self.uuid:
                    self.nodes_db.delete_node(node_id)

            case 'PREPARE':
                coordinator_id = message.get('UUID')
                transaction_id = message.get('GTID')
                transaction = message.get('data').get('transaction')
                self.transactions_db.insert_transaction(transaction_id, coordinator_id, command, transaction)
                check_result, check_text = self.transactions_db.check_transaction(transaction)
                if not check_result or self.name == 'node5':
                    logger.warning(f'{self.__client_indent}{self.name}. Transaction denied: transaction conflict')
                    self.transactions_db.update_transaction_state(transaction_id, 'ABORTED')
                    answer_command = 'DENY'
                else:
                    self.transactions_db.update_transaction_state(transaction_id, answer_command)

            case 'PRECOMMIT':
                if self.name == 'node4':
                    self.stop()
                    return None
                transaction_id = message.get('GTID')
                self.transactions_db.update_transaction_state(transaction_id, command)
                transaction = message.get('data').get('transaction')
                check_result, check_text = self.transactions_db.check_transaction(transaction)
                if not check_result:
                    logger.warning(f'{self.__client_indent}{self.name}. Transaction denied: {check_text}')
                    self.transactions_db.update_transaction_state(transaction_id, 'ABORTED')
                    answer_command = 'DENY'
                else:
                    self.transactions_db.save_to_binlog(transaction_id, transaction)
                    self.transactions_db.update_transaction_state(transaction_id, answer_command)

            case 'COMMIT':
                transaction_id = message.get('GTID')
                self.transactions_db.update_transaction_state(transaction_id, command)
                self.transactions_db.commit_transaction(transaction_id)
                self.transactions_db.update_transaction_state(transaction_id, answer_command)
                logger.exception(f'{self.__client_indent}{self.name}. Transaction commited: {transaction_id}', exc_info=False)

            case 'ABORT':
                transaction_id = message.get('GTID')
                state = self.transactions_db.get_transaction_state(transaction_id)
                if state in ('PRECOMMIT', 'PRECOMMITED', 'COMMIT'):
                    self.transactions_db.delete_from_binlog(transaction_id)
                self.transactions_db.update_transaction_state(transaction_id, 'ABORTED')

        message: dict = {
            **message,
            'UUID': self.uuid,
            'COMMAND': answer_command,
            'data': answer_data,
        }
        self.client_log(message['COMMAND'], addr, message, received=False)
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
        logger.warning(f'Starting server {self.name} with id: {self.uuid} address: {self.address}')
        asyncio.run(self.start_server())

    def stop(self):
        logger.warning(f'Stopped server {self.name} with id: {self.uuid} address: {self.address}')
        os.kill(os.getpid(), 9)
