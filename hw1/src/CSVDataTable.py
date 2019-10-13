from src.BaseDataTable import BaseDataTable
import copy
import csv
import logging
import json
import os
import pandas as pd

pd.set_option("display.width", 256)
pd.set_option('display.max_columns', 20)


class CSVDataTable(BaseDataTable):
    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    """

    _rows_to_print = 10
    _no_of_separators = 2

    def __init__(self, table_name, connect_info, key_columns, debug=True, load=True, rows=None):
        """

        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """
        if key_columns is not None and len(key_columns) != len(set(key_columns)):
            raise ValueError("Duplicated key columns")
        self._data = {
            "table_name": table_name,
            "connect_info": connect_info,
            "key_columns": key_columns,
            "table_columns": None,
            "debug": debug
        }

        self._logger = logging.getLogger()
        self._logger.debug("CSVDataTable.__init__: data = " + json.dumps(self._data, indent=2))

        if rows is not None:
            self._rows = copy.copy(rows)
        else:
            self._rows = []
            self._load()
        if self._rows:
            self._data["table_columns"] = list(self._rows[0].keys())
        self.validate_columns()

    @staticmethod
    def validate_fields(source, target):
        """
        Validate if the source are in the target
        """
        if source is None:
            return
        if not set(source).issubset(set(target)):
            raise ValueError("The columns not in table")

    def validate_columns(self):
        """
        Validate the table columns and key columns
        and check if every key column is in table columns.
        """
        table_columns = self._data.get("table_columns")
        key_columns = self._data.get("key_columns")
        if table_columns is None:
            raise ValueError("Error: No table columns.")
        if key_columns is None:
            self._data["key_columns"] = {}
            return
        CSVDataTable.validate_fields(key_columns, table_columns)
        primary_keys = []
        for r in self._rows:
            pk = [r[k] for k in key_columns]
            if pk in primary_keys:
                raise ValueError("Duplicated primary key")
            primary_keys.append(pk)

    def __str__(self):

        result = "CSVDataTable: config data = \n" + json.dumps(self._data, indent=2)

        no_rows = len(self._rows)
        if no_rows <= CSVDataTable._rows_to_print:
            rows_to_print = self._rows[0:no_rows]
        else:
            temp_r = int(CSVDataTable._rows_to_print / 2)
            rows_to_print = self._rows[0:temp_r]
            keys = self._rows[0].keys()

            for i in range(0, CSVDataTable._no_of_separators):
                tmp_row = {}
                for k in keys:
                    tmp_row[k] = "***"
                rows_to_print.append(tmp_row)

            rows_to_print.extend(self._rows[int(-1 * temp_r) - 1:-1])

        df = pd.DataFrame(rows_to_print)
        result += "\nSome Rows: = \n" + str(df)

        return result

    def _add_row(self, r):
        self._rows.append(r)

    def _load(self):

        dir_info = self._data["connect_info"].get("directory")
        file_n = self._data["connect_info"].get("file_name")
        full_name = os.path.join(dir_info, file_n)

        with open(full_name, "r") as txt_file:
            csv_d_rdr = csv.DictReader(txt_file, delimiter=self._data["connect_info"].get("delimiter", ","))
            for r in csv_d_rdr:
                self._add_row(r)

        self._logger.debug("CSVDataTable._load: Loaded " + str(len(self._rows)) + " rows")

    def save(self):
        """
        Write the information back to a file.
        :return: None
        """
        dir_info = self._data["connect_info"].get("directory")
        file_n = self._data["connect_info"].get("file_name")
        full_name = os.path.join(dir_info, file_n)
        fieldnames = self._data["table_columns"]
        if fieldnames is None:
            return
        with open(full_name, 'w') as txt_file:
            csv_d_wtr = csv.DictWriter(txt_file, fieldnames=fieldnames)
            csv_d_wtr.writeheader()
            for r in self._rows:
                csv_d_wtr.writerow(r)

    @staticmethod
    def matches_template(row, template):

        result = True
        if template is not None:
            for k, v in template.items():
                if v != row.get(k, None):
                    result = False
                    break

        return result

    @staticmethod
    def _project(row, field_list=None):
        """
        Select the row's fields only in the field list.
        """
        if field_list is None:
            return row
        result = {}
        for k in field_list:
            result[k] = row[k]
        return result

    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the requested fields for the record identified
            by the key.
        """
        key_columns = self._data["key_columns"]
        if not key_columns or key_fields is None or len(key_columns) != len(key_fields):
            raise ValueError("Not a valid primary key")
        template = dict(zip(key_columns, key_fields))
        result = self.find_by_template(template, field_list)
        length = len(result)
        if length > 1:
            raise ValueError("Duplicate primary key or not a primary key")
        elif length == 1:
            return result[0]
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
        if template is None:
            return self._rows
        CSVDataTable.validate_fields(template.keys(), self._data["table_columns"])
        CSVDataTable.validate_fields(field_list, self._data["table_columns"])
        result = []
        for r in self._rows:
            if CSVDataTable.matches_template(r, template):
                r = CSVDataTable._project(r, field_list)
                result.append(r)
        return result

    def delete_by_key(self, key_fields):
        """

        Deletes the record that matches the key.

        :param template: A template.
        :return: A count of the rows deleted.
        """
        record = self.find_by_primary_key(key_fields)
        if record is None:
            return 0
        self._rows.remove(record)
        return 1

    def delete_by_template(self, template):
        """

        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """
        if template is None:
            return 0
        records = self.find_by_template(template)
        self._rows = [r for r in self._rows if r not in records]
        return len(records)

    @staticmethod
    def update_row(row, new_values):
        """
        Update the row with new values.
        """
        new_row = {k: new_values[k] if k in new_values else row[k] for k in row}
        return new_row

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """
        tmp = dict(zip(self._data["key_columns"], key_fields))
        return self.update_by_template(tmp, new_values)

    def update_by_template(self, template, new_values):
        """

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        """
        if new_values is None or len(new_values) == 0:
            return 0
        self.validate_fields(new_values.keys(), self._data["table_columns"])
        count = 0
        backup = self._rows
        new_records = self.find_by_template(template)
        for r in new_records:
            idx = self._rows.index(r)
            new_row = CSVDataTable.update_row(r, new_values)
            key = [new_row[k] for k in self._data["key_columns"]]
            self._rows[idx] = new_row
            try:
                self.find_by_primary_key(key)
                count += 1
            except Exception as e:
                self._rows = backup
                raise e
        return count

    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """
        if new_record is None:
            return
        self.validate_fields(new_record.keys(), self._data["table_columns"])
        backup = self._rows
        key = [new_record[k] for k in self._data["key_columns"]]
        self._rows.append(new_record)
        try:
            self.find_by_primary_key(key)
        except Exception as e:
            self._rows = backup
            raise e
        print("Insert successfully.")

    def get_rows(self):
        return self._rows
