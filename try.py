from dataclasses import dataclass
from dataclasses_json import dataclass_json
from db_api import DBField, DBTable, SelectionCriteria
from typing import Any, Dict, List, Type
import shelve
import os


@dataclass_json
@dataclass
class DBField(DBField):
    def __init__(self, name, type):
        self.name = name
        self.type = type

@dataclass_json
@dataclass
class SelectionCriteria(SelectionCriteria):
    def __init__(self, field_name, operator, value):
        self.field_name = field_name
        self.operator = operator
        self.value = value


@dataclass_json
@dataclass
class DBTable(DBTable):
    def __init__(self, name, fields, key_field_name):
        if key_field_name not in fields:
            raise KeyError
        self.name = name
        self.fields = fields
        self.key_field_name = key_field_name
        with shelve.open(f"{'DB'}\\{self.name}.db") as data_file:
            pass

    def count(self) -> int:
        with shelve.open(f"{'DB'}\\{self.name}.db") as data_file:
            return len(data_file)

    def insert_record(self, values: Dict[str, Any]) -> None:
        # check the correctness of the keys
        if self.key_field_name not in values:
            raise KeyError("the key value didn't given")

        for key in values.keys():
            if key not in self.fields:
                raise KeyError(f"the key {key} is not exists in the key fields")

        # add the row

        with shelve.open(f"{'DB'}/{self.name}.db") as data_file:
            try:
                for row in data_file:
                    if row[self.key_field_name] == values[self.key_field_name]:
                        raise ValueError

                data_file[values.keys()] = values.values()
            finally:
                data_file.close()

    def delete_record(self, key: Any) -> None:
        with shelve.open(f"{'DB'}/{self.name}.db") as my_data:
            for row in my_data:
                if row[self.key_field_name] == key:
                    del my_data[row]
            raise ValueError

    def delete_records(self, criteria: List[SelectionCriteria]) -> None:
        raise NotImplementedError

    def get_record(self, key: Any) -> Dict[str, Any]:
        with shelve.open(f"{'DB'}/{self.name}.db") as my_data:
            for row in my_data:
                if row[self.key_field_name] == key:
                    return my_data[row]
            raise ValueError

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        my_data = shelve.open(f"{'DB'}/{self.name}.db")
        try:
            my_data[key] = values
        finally:
            my_data.close()


    def query_table(self, criteria: List[SelectionCriteria]) \
            -> List[Dict[str, Any]]:
        raise NotImplementedError


    def create_index(self, field_to_index: str) -> None:
        raise NotImplementedError


@dataclass_json
@dataclass
class DataBase:
    # Put here any instance information needed to support the API
    db_table = {}

    def create_table(self,
                     table_name: str,
                     fields: List[DBField],
                     key_field_name: str) -> DBTable:

        new_table = DBTable(table_name, fields, key_field_name)
        self.db_table[table_name] = new_table
        return new_table

    def num_tables(self) -> int:
        return len([name for name in os.listdir("DB") if os.path.isfile(os.path.join("DB", name))])

    def get_table(self, table_name: str) -> DBTable:
        with shelve.open(f"{'DB'}/{table_name}.db") as my_data:
            return my_data

    def delete_table(self, table_name: str) -> None:
        if os.path.exists(f"{'DB'}\\{table_name}.db"):
            os.remove(f"{'DB'}\\{table_name}.db")
        else:
            print("The table does not exist")

    def get_tables_names(self) -> List[Any]:
        table_names = []
        for root, dirs, files in os.walk("DB"):
            for file in files:
                table_names.append(file)
        return table_names

    def query_multiple_tables(
            self,
            tables: List[str],
            fields_and_values_list: List[List[SelectionCriteria]],
            fields_to_join_by: List[str]
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError
