from typing import Optional
from datetime import datetime
from pydantic import BaseModel, Field
from dynamodb_model import DynamoDBModel

# Example Usage:
class UserModel(BaseModel):
    user_id: str = Field(alias='partition_key')
    email: str = Field(alias='sort_key')
    name: str
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: Optional[datetime] = None

# Initialize the wrapper
user_db = DynamoDBModel[UserModel](
    table_name='users',
    model_class=UserModel
)

# Usage examples:
def example_usage():
    new_user = UserModel(
        user_id="user123",
        email="user@example.com",
        name="John Doe"
    )
    # Create
    user_db.put(new_user)
    
    # Read
    user = user_db.get("user123", "user@example.com")
    
    # Query
    users = user_db.query(
        partition_key="user123",
        sort_key_condition={
            "operator": "begins_with",
            "value": "user"
        },
        filter_expression={
            "name": {
                "operator": "contains",
                "value": "John"
            }
        }
    )
    
    # Update
    updated_user = user_db.update(
        partition_key="user123",
        sort_key="user@example.com",
        update_data={
            "name": "John Smith",
            "updated_at": datetime.now()
        }
    )
    
    # Delete
    user_db.delete("user123", "user@example.com")