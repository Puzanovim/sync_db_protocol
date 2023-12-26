import logging
from contextlib import contextmanager
from sqlite3 import Cursor, connect
from uuid import UUID

from orjson import orjson

from src.app.exceptions import NodeNotFound
from src.app.schemas import Address


logger = logging.getLogger(__name__)


def bin_to_hex(value) -> str:
    if isinstance(value, bytes):
        return value.hex()

    return value


class NodeRepository:
    def __init__(self, name_db: str):
        self._db_address = name_db

    @contextmanager
    def transaction(self):
        connection = connect(self._db_address)
        try:
            with connection as conn:
                yield conn.cursor()
        except Exception as exc:
            logger.warning(f'Exception found: {exc}')
            connection.rollback()
        else:
            connection.commit()
        finally:
            connection.close()

    def init_db(self) -> None:
        with self.transaction() as cursor:
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS node (
                id INTEGER PRIMARY KEY,
                node TEXT NOT NULL,
                address TEXT NOT NULL
            )
            ''')
            cursor.close()

    def get_address_by_node(self, node: UUID) -> Address:
        with self.transaction() as cursor:
            cursor.execute(
                """
                SELECT `address` FROM node WHERE node = :node
                LIMIT 1
            """,
                {"node": str(node)},
            )
            result = cursor.fetchone()
            cursor.close()

        if result is None:
            raise NodeNotFound

        (address_str,) = result

        return Address.model_validate(address_str)

    def get_nodes(self) -> dict[str, Address]:
        with self.transaction() as cursor:
            cursor.execute(
                """
                    SELECT id, node, address FROM node
                """
            )
            return {row[1]: Address.model_validate(orjson.loads(row[2])) for row in cursor.fetchall()}

    def get_node_by_address(self, address: Address) -> str:
        address_str: str = self._dumps_dict(address.model_dump_json())

        with self.transaction() as cursor:
            cursor.execute(
                """
                SELECT `node` FROM node WHERE address = :address
                LIMIT 1
            """,
                {"address": address_str},
            )
            result = cursor.fetchone()
            cursor.close()

        if result is None:
            raise NodeNotFound

        (node_id,) = result

        return node_id

    def insert_node(self, node: str, address: Address) -> int:
        address_str: str = self._dumps_dict(address.model_dump_json())

        with self.transaction() as cursor:
            result: Cursor = cursor.execute(
                """
                INSERT INTO node 
                    (node, address)
                VALUES 
                    (:node, :address)
            """,
                {"node": str(node), "address": address_str},
            )
            cursor.close()

        return result.lastrowid

    def _dumps_dict(self, some_dict: str | dict) -> str:
        if isinstance(some_dict, str):
            return some_dict

        return str(orjson.dumps(some_dict, default=bin_to_hex))
