
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
        self._data = {
            "table_name": table_name,
            "connect_info": connect_info,
            "key_columns": key_columns,
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

    def validate_columns(self):
        table_columns = self._data.get("table_columns")
        key_columns = self._data.get("key_columns")
        if not table_columns:
            raise Exception("Invalid DataTable.")
        if not key_columns:
            raise Exception("No key columns.")
        for c in key_columns:
            if c not in table_columns:
                raise Exception("Invalid key columns.")
        # TODO: check if key_columns are duplicated

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

            rows_to_print.extend(self._rows[int(-1*temp_r)-1:-1])

        df = pd.DataFrame(rows_to_print)
        result += "\nSome Rows: = \n" + str(df)

        return result

    def _add_row(self, r):
        if self._rows is None:
            self._rows = []
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
        with open(full_name, 'w') as txt_file:
            csv_d_wtr = csv.DictWriter(txt_file, fieldnames=self._data["key_columns"])
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

    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the requested fields for the record identified
            by the key.
        """
        template = dict(zip(self._data["key_columns"], key_fields))
        result = self.find_by_template(template)
        if len(result) == 1:
            return result[0]
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
        result = []
        for r in self._rows:
            if self.matches_template(r, template):
                if field_list is not None:
                    r = {k: r[k] for k in field_list}
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
        records = self.find_by_template(template)
        self._rows = [r for r in self._rows if r not in records]
        return len(records)

    @staticmethod
    def update_row(row, new_values):
        new_row = {k: new_values[k] if k in new_values else row[k] for k in row}
        return new_row

    def validate_primary_key(self, new_record):
        primary_key = {k: new_record[k] for k in self._data["key_columns"]}
        if self.find_by_template(primary_key):
            raise Exception("Duplicate primary key.")

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """
        row = self.find_by_primary_key(key_fields)
        idx = self._rows.index(row)
        if row is None:
            return 0
        new_row = self.update_row(row, new_values)
        self.validate_primary_key(new_row)
        self._rows[idx] = new_row
        return 1

    # TODO: how to update
    def update_by_template(self, template, new_values):
        """

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        """
        for r in self.find_by_template(template):
            new_r = self.update_row(r, new_values)
            self.validate_primary_key(new_r)
        count = 0
        for idx, r in enumerate(self._rows):
            if self.matches_template(r, template):
                new_r = self.update_row(r, new_values)
                self._rows[idx] = new_r
                count += 1
        return count

    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """
        self.validate_primary_key(new_record)
        self._rows.append(new_record)

    def get_rows(self):
        return self._rows
