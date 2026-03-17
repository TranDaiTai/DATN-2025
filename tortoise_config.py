TORTOISE_ORM = {
    "connections": {"default": "postgres://postgres:123@localhost:5432/postgres"},
    "apps": {
        "models": {
            "models": ["src.models.orm_models", "aerich.models"],
            "default_connection": "default",
        }
    },
}
