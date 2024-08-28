import datetime
import json
import uuid
from peewee import SqliteDatabase, Model, CharField, TextField, \
    DateTimeField, ForeignKeyField, IntegerField, UUIDField, BooleanField
import os


current_dir = os.path.dirname(os.path.abspath(__file__))
db = SqliteDatabase(os.path.join(current_dir, 'data.db'), pragmas={'journal_mode': 'wal'}, check_same_thread=False)


class JSONField(TextField):
    def python_value(self, value):
        if value is not None:
            return json.loads(value)
        return value

    def db_value(self, value):
        if value is not None:
            return json.dumps(value)
        return value


class BaseModel(Model):
    class Meta:
        database = db


class ParsingItem(BaseModel):
    user_id = CharField()
    link = CharField(unique=True)


class App(BaseModel):
    appid = UUIDField(primary_key=True, default=uuid.uuid4)
    name = CharField()


class Crawl(BaseModel):
    crawlid = UUIDField(primary_key=True, default=uuid.uuid4)
    created_at = DateTimeField(default=datetime.datetime.now)
    finished = BooleanField(default=False)


class Product(BaseModel):
    appid = ForeignKeyField(App)
    crawlid = ForeignKeyField(Crawl)
    productId = CharField(16)
    name = CharField()
    in_stock = CharField()
    brandName = CharField(null=True)
    price = IntegerField()
    category = CharField()
    details = JSONField()


if not db.table_exists(Product):
    db.create_tables(BaseModel.__subclasses__())
