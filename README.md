# Python DynamoDB ORM

A lightweight and type-safe wrapper for AWS DynamoDB with Pydantic integration.

## Features

- Type-safe DynamoDB operations using Pydantic models
- Support for CRUD operations (Create, Read, Update, Delete)
- Query filtering and conditional operations
- Clean and intuitive API design
- Comprehensive error handling

## Installation

```bash
git clone https://github.com/leehjhjhj/dynamodb-model
```

```bash
uv sync
```

## Quick Start

```python
from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional

# Define your model
class UserModel(BaseModel):
    user_id: str = Field(alias='partition_key')
    email: str = Field(alias='sort_key')
    name: str
    age: int
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: Optional[datetime] = None

# Initialize DynamoDB wrapper
user_db = DynamoDBModel[UserModel](
    table_name='users',
    model_class=UserModel
)

# Create a new user
new_user = UserModel(
    user_id="user123",
    email="user@example.com",
    name="John Doe",
    age=30
)
user_db.put(new_user)

# Get user
user = user_db.get("user123", "user@example.com")

# Query users
users = user_db.query(
    partition_key="user123",
    sort_key_condition={
        "operator": "begins_with",
        "value": "user"
    }
)
```

## Requirements

- Python 3.7+
- boto3
- pydantic

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.