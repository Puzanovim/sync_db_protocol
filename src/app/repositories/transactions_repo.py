import logging
from contextlib import contextmanager
from sqlite3 import Cursor, connect
from uuid import UUID

from orjson import orjson

from src.app.exceptions import TransactionNotFound

logger = logging.getLogger(__name__)


def bin_to_hex(value) -> str:
    if isinstance(value, bytes):
        return value.hex()

    return value


class TransactionsRepository:
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
            CREATE TABLE IF NOT EXISTS transaction_table (
                id INTEGER PRIMARY KEY,
                gtid TEXT NOT NULL,
                coordinator_id TEXT NOT NULL,
                transaction_state TEXT NOT NULL,
                transaction_text TEXT NOT NULL
            )
            ''')
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS binlog (
                id INTEGER PRIMARY KEY,
                gtid TEXT NOT NULL,
                transaction_text TEXT NOT NULL
            )
            ''')
            cursor.close()

    def get_transactions(self) -> dict[str, tuple[str, ...]]:
        with self.transaction() as cursor:
            cursor.execute(
                """
                SELECT gtid, coordinator_id, transaction_state, transaction_text FROM transaction_table
                """
            )
            return {row[0]: (row[1], row[2]) for row in cursor.fetchall()}

    def get_full_transactions(self) -> dict[str, tuple[str, ...]]:
        with self.transaction() as cursor:
            cursor.execute(
                """
                SELECT gtid, coordinator_id, transaction_state, transaction_text FROM transaction_table
                """
            )
            return {row[0]: (row[1], row[2], row[3]) for row in cursor.fetchall()}

    def check_transaction(self, transaction: str) -> tuple[bool, str | None]:
        if len(transaction) > 270:
            return False, 'Conflict with transaction'

        return True, None


    def commit_transaction(self, gtid: UUID) -> None:
        pass

    def get_transaction_state(self, gtid: UUID) -> str:
        with self.transaction() as cursor:
            cursor.execute(
                """
                SELECT transaction_state FROM transaction_table WHERE gtid = :gtid
                LIMIT 1
            """,
                {"gtid": str(gtid)},
            )
            result = cursor.fetchone()
            cursor.close()

        if result is None:
            raise TransactionNotFound

        (transaction_state,) = result

        return transaction_state

    def update_transaction_state(self, gtid: UUID, state: str) -> None:
        with self.transaction() as cursor:
            cursor.execute(
                """
                UPDATE transaction_table SET transaction_state = :state WHERE gtid = :gtid
                """,
                {"gtid": str(gtid), "state": state},
            )
            cursor.close()

    def insert_transaction(self, gtid: UUID, coordinator_id: UUID, state: str, transaction: str) -> int:
        with self.transaction() as cursor:
            result: Cursor = cursor.execute(
                """
                INSERT INTO transaction_table 
                    (gtid, coordinator_id, transaction_state, transaction_text)
                VALUES 
                    (:gtid, :coordinator_id, :state, :transaction)
            """,
                {
                    "gtid": str(gtid),
                    "coordinator_id": str(coordinator_id),
                    "state": state,
                    "transaction": transaction
                },
            )
            cursor.close()

        return result.lastrowid

    def delete_transaction(self, gtid: str) -> None:
        with self.transaction() as cursor:
            result: Cursor = cursor.execute(
                """
                DELETE FROM transaction_table WHERE gtid = :gtid
                """,
                {"gtid": gtid},
            )
            cursor.close()

    def get_transaction_from_binlog(self, gtid: UUID) -> str:
        with self.transaction() as cursor:
            cursor.execute(
                """
                SELECT transaction_text FROM binlog WHERE gtid = :gtid
                LIMIT 1
            """,
                {"gtid": str(gtid)},
            )
            result = cursor.fetchone()
            cursor.close()

        if result is None:
            raise TransactionNotFound

        (transaction,) = result

        return transaction

    def save_to_binlog(self, gtid: UUID, transaction: str) -> int:
        with self.transaction() as cursor:
            result: Cursor = cursor.execute(
                """
                INSERT INTO binlog 
                    (gtid, transaction_text)
                VALUES 
                    (:gtid, :transaction)
            """,
                {"gtid": str(gtid), "transaction": transaction},
            )
            cursor.close()

        return result.lastrowid

    def delete_from_binlog(self, gtid: UUID) -> None:
        with self.transaction() as cursor:
            result: Cursor = cursor.execute(
                """
                DELETE FROM binlog WHERE gtid = :gtid
                """,
                {"gtid": str(gtid)},
            )
            cursor.close()

    def _dumps_dict(self, some_dict: str | dict) -> str:
        if isinstance(some_dict, str):
            return some_dict

        return str(orjson.dumps(some_dict, default=bin_to_hex))
