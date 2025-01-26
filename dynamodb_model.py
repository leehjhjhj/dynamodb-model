from typing import Optional, Dict, Any, Type, TypeVar, Generic
from contextlib import contextmanager
from datetime import datetime
from pydantic import BaseModel, Field
import boto3
from boto3.dynamodb.conditions import Key, Attr

T = TypeVar('T', bound=BaseModel)

class DynamoDBModel(Generic[T]):
    def __init__(
        self,
        table_name: str,
        model_class: Type[T],
        region: str = "ap-northeast-2",
    ):
        self.table_name = table_name
        self.model_class = model_class
        self.dynamodb = boto3.resource('dynamodb', region_name=region)
        self.table = self.dynamodb.Table(table_name)
        self.partition_key_field = None
        self.sort_key_field = None

        for field_name, field in model_class.model_fields.items():
            if field.alias == 'partition_key':
                self.partition_key_field = field_name
            elif field.alias == 'sort_key':
                self.sort_key_field = field_name

    def get(self, partition_key: str, sort_key: Optional[str] = None) -> Optional[T]:
        key_condition = {self.partition_key_field: partition_key}
        if sort_key:
            key_condition[self.sort_key_field] = sort_key
            
        try:
            response = self.table.get_item(Key=key_condition)
            item = response.get('Item')
            return self._item_to_model(item) if item else None
        except Exception as e:
            raise Exception(f"Failed to get item: {str(e)}")

    def put(self, model: T) -> T:
        try:
            item = self._model_to_item(model)
            self.table.put_item(Item=item)
            return model
        except Exception as e:
            raise Exception(f"Failed to put item: {str(e)}")

    def query(
        self,
        partition_key: str,
        sort_key_condition: Optional[Dict[str, Any]] = None,
        filter_expression: Optional[Dict[str, Any]] = None,
    ) -> list[T]:
        try:
            key_condition = Key(self.partition_key_field).eq(partition_key)
            
            if sort_key_condition:
                operator = sort_key_condition.get('operator', 'eq')
                value = sort_key_condition['value']
                
                if operator == 'begins_with':
                    key_condition = key_condition & Key(self.sort_key_field).begins_with(value)
                elif operator == 'between':
                    key_condition = key_condition & Key(self.sort_key_field).between(*value)
                else:
                    key_condition = key_condition & getattr(Key(self.sort_key_field), operator)(value)

            query_args = {
                'KeyConditionExpression': key_condition
            }

            if filter_expression:
                filter_conditions = []
                for key, condition in filter_expression.items():
                    operator = condition.get('operator', 'eq')
                    value = condition['value']
                    filter_conditions.append(
                        getattr(Attr(key), operator)(value)
                    )
                
                final_filter = filter_conditions[0]
                for condition in filter_conditions[1:]:
                    final_filter = final_filter & condition
                
                query_args['FilterExpression'] = final_filter

            response = self.table.query(**query_args)
            items = response.get('Items', [])
            
            return [self._item_to_model(item) for item in items]
        except Exception as e:
            raise Exception(f"Failed to query items: {str(e)}")

    def update(
        self,
        partition_key: str,
        sort_key: Optional[str] = None,
        update_data: Dict[str, Any] = None
    ) -> Optional[T]:
        try:
            key_condition = {'partition_key': partition_key}
            if sort_key:
                key_condition['sort_key'] = sort_key

            update_expression = []
            expression_attribute_values = {}
            expression_attribute_names = {}

            for key, value in update_data.items():
                placeholder = f":val_{key}"
                attr_name = f"#{key}"
                update_expression.append(f"{attr_name} = {placeholder}")
                expression_attribute_values[placeholder] = value
                expression_attribute_names[attr_name] = key

            update_args = {
                'Key': key_condition,
                'UpdateExpression': f"SET {', '.join(update_expression)}",
                'ExpressionAttributeValues': expression_attribute_values,
                'ExpressionAttributeNames': expression_attribute_names,
                'ReturnValues': 'ALL_NEW'
            }

            response = self.table.update_item(**update_args)
            new_item = response.get('Attributes')
            
            return self._item_to_model(new_item) if new_item else None
        except Exception as e:
            raise Exception(f"Failed to update item: {str(e)}")

    def delete(self, partition_key: str, sort_key: Optional[str] = None) -> None:
        try:
            key_condition = {'partition_key': partition_key}
            if sort_key:
                key_condition['sort_key'] = sort_key
                
            self.table.delete_item(Key=key_condition)
        except Exception as e:
            raise Exception(f"Failed to delete item: {str(e)}")

    def _model_to_item(self, model: T) -> Dict[str, Any]:
        item: Dict[str, Any] = {}
        for k, v in model.model_dump().items():
            if v is not None:
                converted_datetime = self._convert_datetime_to_str(v)
                item[k] = converted_datetime
        return item
    
    def _item_to_model(self, item: Dict[str, Any]) -> T:
        return self.model_class(**item)
    
    def _convert_datetime_to_str(self, value: Any) -> Any:
        if isinstance(value, datetime):
            return value.isoformat()
        return value