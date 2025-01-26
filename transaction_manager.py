from typing import Dict, Any, List
import boto3
from contextlib import contextmanager

class TransactionManager:
    def __init__(self, region: str = "ap-northeast-2"):
        self.client = boto3.client('dynamodb', region_name=region)
        self._transaction_items: List[Dict[str, Any]] = []

    def add_transaction_item(self, item: Dict[str, Any]):
        self._transaction_items.append(item)

    def commit(self):
        if self._transaction_items:
            print(self._transaction_items)
            self.client.transact_write_items(TransactItems=self._transaction_items)
        self._transaction_items.clear()

    def rollback(self):
        self._transaction_items.clear()

class TransactionScope:
    def __init__(self, *models):
        self.models = models
        self.region = "ap-northeast-2"
        
    @contextmanager
    def transaction(self):
        tx_manager = TransactionManager(region=self.region)
        try:
            for model in self.models:
                model.set_transaction_manager(tx_manager)
            yield tx_manager
            tx_manager.commit()
        except Exception as e:
            tx_manager.rollback()
            raise Exception(f"Transaction failed: {str(e)}")
        finally:
            for model in self.models:
                model.set_transaction_manager(None)