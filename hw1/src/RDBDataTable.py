import json
import pandas as pd
from src import dbutils
from src.BaseDataTable import BaseDataTable


class RDBDataTable(BaseDataTable):
    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    """

    def __init__(self, table_name, connect_info, key_columns):
        """

        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """
        if table_name is None or connect_info is None:
            raise ValueError("Invalid input.")
        if key_columns is not None and len(key_columns) != len(set(key_columns)):
            raise ValueError("Duplicated key columns")

        self._data = {
            "table_name": table_name,
            "connect_info": connect_info,
            "key_columns": key_columns,
            "table_columns": None
        }

        cnx = dbutils.get_connection(connect_info)
        if cnx is not None:
            self._cnx = cnx
        else:
            raise Exception("Could not get a connection.")
        sql = f"SHOW KEYS FROM {self._data['table_name']} WHERE Key_name = 'PRIMARY'"
        _, data = dbutils.run_q(sql, conn=self._cnx)
        primary_key = [d[4] for d in data]
        if key_columns is None:
            self._data["key_columns"] = primary_key
        elif set(key_columns) != set(primary_key):
            raise ValueError("Invalid primary key: not consistent with database")
        sql = f"SHOW COLUMNS FROM {self._data['table_name']}"
        _, data = dbutils.run_q(sql, conn=self._cnx)
        self._data["table_columns"] = [d[0] for d in data]

    def __str__(self):

        result = "RDBDataTable:\n"
        result += json.dumps(self._data, indent=2)

        row_count = self.get_row_count()
        result += "\nNumber of rows = " + str(row_count)

        some_rows = pd.read_sql(
            "select * from " + self._data["table_name"] + " limit 10",
            con=self._cnx
        )
        result += "First 10 rows = \n"
        result += str(some_rows)

        return result

    def get_row_count(self):

        row_count = self._data.get("row_count", None)
        if row_count is None:
            sql = "select count(*) as count from " + self._data["table_name"]
            res, d = dbutils.run_q(sql, args=None, fetch=True, conn=self._cnx, commit=True)
            row_count = d[0][0]
            self._data['"row_count'] = row_count

        return row_count

    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the requested fields for the record identified
            by the key.
        """
        try:
            template = dbutils.zip_to_template(self._data['key_columns'], key_fields)
            res = self.find_by_template(template, field_list)
        except Exception as e:
            raise e
        if len(res) > 1:
            raise ValueError("Not a primary key")
        elif len(res) == 1:
            return res[0]
        else:
            return None

    def find_by_template(self, template, field_list=None, limit=None, offset=None, order_by=None):
        """

        :param template: A dictionary of the form { "field1" : value1, "field2": value2, ...}
        :param field_list: A list of request fields of the form, ['fielda', 'fieldb', ...]
        :param limit: Do not worry about this for now.
        :param offset: Do not worry about this for now.
        :param order_by: Do not worry about this for now.
        :return: A list containing dictionaries. A dictionary is in the list representing each record
            that matches the template. The dictionary only contains the requested fields.
        """
        sql, args = dbutils.create_select(self._data["table_name"], template, field_list)
        try:
            _, data = dbutils.run_q(sql, args, conn=self._cnx)
        except Exception as e:
            raise e
        result = []
        for values in data:
            row = dict(zip(self._data["table_columns"], values))
            result.append(row)
        return result

    def delete_by_key(self, key_fields):
        """

        Deletes the record that matches the key.

        :param template: A template.
        :return: A count of the rows deleted.
        """
        try:
            template = dbutils.zip_to_template(self._data['key_columns'], key_fields)
            res = self.delete_by_template(template)
        except Exception as e:
            raise e
        return res

    def delete_by_template(self, template):
        """

        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """
        if template is None:
            return 0
        sql, args = dbutils.create_delete(self._data["table_name"], template)
        try:
            res, _ = dbutils.run_q(sql, args, conn=self._cnx)
        except Exception as e:
            raise e
        return res

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """
        try:
            template = dbutils.zip_to_template(self._data['key_columns'], key_fields)
            res = self.update_by_template(template, new_values)
        except Exception as e:
            raise e
        return res

    def update_by_template(self, template, new_values):
        """

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        """
        if template is None:
            return 0
        sql, args = dbutils.create_update(self._data["table_name"], new_values, template)
        try:
            res, _ = dbutils.run_q(sql, args, conn=self._cnx)
        except Exception as e:
            raise e
        return res

    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """
        sql, args = dbutils.create_insert(self._data["table_name"], new_record)
        try:
            dbutils.run_q(sql, args, conn=self._cnx)
        except Exception as e:
            raise e
        print("Insert successfully.")
