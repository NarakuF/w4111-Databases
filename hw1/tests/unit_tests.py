from src.CSVDataTable import CSVDataTable
from src.RDBDataTable import RDBDataTable
import os
import time

data_dir = os.path.abspath("../Data/Baseball")
table_name = "People"
csv_connect_info = {
    "directory": data_dir,
    "file_name": "people.csv"
}
rdb_connect_info = {'host': 'localhost',
                   'port': 3306,
                   'user': 'dbuser',
                   'password': 'dbuserdbuser',
                   'db': 'lahman2019raw'}
key_columns = ["playerID"]


def test_init():
    print("\n******************** " + "test_init" + " ********************")

    print("\nCompare time elapsed for initialization.")
    start1 = time.time()
    _ = CSVDataTable(table_name, csv_connect_info, key_columns)
    end1 = time.time()
    elapsed1 = end1 - start1
    print("Time elapsed for CSVDataTable is ", elapsed1, "seconds.")

    start2 = time.time()
    _ = RDBDataTable(table_name, rdb_connect_info, key_columns)
    end2 = time.time()
    elapsed2 = end2 - start2
    print("Time elapsed for RDBDataTable is ", elapsed2, "seconds.")

    print("******************** " + "end test_init" + " ********************\n")


def test_find_by_primary_key():
    print("\n******************** " + "test_find_by_primary_key" + " ********************")

    print("Compare time elapsed for find_by_primary_key.")
    csv_tbl = CSVDataTable(table_name, csv_connect_info, key_columns)
    rdb_tbl = RDBDataTable(table_name, rdb_connect_info, key_columns)

    start1 = time.time()
    r1 = csv_tbl.find_by_primary_key(["aardsda01"])
    end1 = time.time()
    elapsed1 = end1 - start1
    print("Time elapsed for CSVDataTable is ", elapsed1, "seconds.")

    start2 = time.time()
    r2 = rdb_tbl.find_by_primary_key(["aardsda01"])
    end2 = time.time()
    elapsed2 = end2 - start2
    print("Time elapsed for RDBDataTable is ", elapsed2, "seconds.")

    print("******************** " + "end test_find_by_primary_key" + " ********************\n")


def test_find_by_template():
    print("\n******************** " + "test_find_by_template" + " ********************")

    print("Compare time elapsed for find_by_template.")
    tmp = {"birthCountry": "USA", "birthState": "MA"}
    csv_tbl = CSVDataTable(table_name, csv_connect_info, key_columns)
    rdb_tbl = RDBDataTable(table_name, rdb_connect_info, key_columns)

    start1 = time.time()
    r1 = csv_tbl.find_by_template(tmp)
    end1 = time.time()
    elapsed1 = end1 - start1
    print("Time elapsed for CSVDataTable is ", elapsed1, "seconds.")

    start2 = time.time()
    r2 = rdb_tbl.find_by_template(tmp)
    end2 = time.time()
    elapsed2 = end2 - start2
    print("Time elapsed for RDBDataTable is ", elapsed2, "seconds.")

    print("******************** " + "end test_find_by_template" + " ********************\n")


def test_delete_by_key():
    print("\n******************** " + "test_delete_by_key" + " ********************")

    print("Compare time elapsed for delete_by_key.")
    csv_tbl = CSVDataTable(table_name, csv_connect_info, key_columns)
    rdb_tbl = RDBDataTable(table_name, rdb_connect_info, key_columns)

    start1 = time.time()
    r1 = csv_tbl.delete_by_key(['aardsda01'])
    end1 = time.time()
    elapsed1 = end1 - start1
    print("Time elapsed for CSVDataTable is ", elapsed1, "seconds.")

    start2 = time.time()
    r2 = rdb_tbl.delete_by_key(['aardsda01'])
    end2 = time.time()
    elapsed2 = end2 - start2
    print("Time elapsed for RDBDataTable is ", elapsed2, "seconds.")

    print("******************** " + "end test_delete_by_key" + " ********************\n")


def test_delete_by_template():
    print("\n******************** " + "test_delete_by_template" + " ********************")

    print("Compare time elapsed for delete_by_template.")
    csv_tbl = CSVDataTable(table_name, csv_connect_info, key_columns)
    rdb_tbl = RDBDataTable(table_name, rdb_connect_info, key_columns)
    tmp = {"birthCountry": "USA", "birthState": "MA", "birthCity": "Boston"}

    start1 = time.time()
    r1 = csv_tbl.delete_by_template(tmp)
    end1 = time.time()
    elapsed1 = end1 - start1
    print("Time elapsed for CSVDataTable is ", elapsed1, "seconds.")

    start2 = time.time()
    r2 = rdb_tbl.delete_by_template(tmp)
    end2 = time.time()
    elapsed2 = end2 - start2
    print("Time elapsed for RDBDataTable is ", elapsed2, "seconds.")

    print("******************** " + "end test_delete_by_template" + " ********************\n")


def test_update_by_key():
    print("\n******************** " + "test_update_by_key" + " ********************")

    print("Compare time elapsed for update_by_key.")
    csv_tbl = CSVDataTable(table_name, csv_connect_info, key_columns)
    rdb_tbl = RDBDataTable(table_name, rdb_connect_info, key_columns)
    value = {"nameFirst": "Jackson", "nameLast": "Copper"}

    start1 = time.time()
    r1 = csv_tbl.update_by_key(["abadijo01"], value)
    end1 = time.time()
    elapsed1 = end1 - start1
    print("Time elapsed for CSVDataTable is ", elapsed1, "seconds.")

    start2 = time.time()
    r2 = rdb_tbl.update_by_key(["abadijo01"], value)
    end2 = time.time()
    elapsed2 = end2 - start2
    print("Time elapsed for RDBDataTable is ", elapsed2, "seconds.")

    print("******************** " + "end test_update_by_key" + " ********************\n")


def test_update_by_template():
    print("\n******************** " + "test_update_by_template" + " ********************")

    print("Compare time elapsed for update_by_template.")
    csv_tbl = CSVDataTable(table_name, csv_connect_info, key_columns)
    rdb_tbl = RDBDataTable(table_name, rdb_connect_info, key_columns)
    tmp = {"birthYear": "1990", "birthCountry": "USA"}
    value = {"nameFirst": "Jackson", "nameLast": "Copper"}

    start1 = time.time()
    r1 = csv_tbl.update_by_template(tmp, value)
    end1 = time.time()
    elapsed1 = end1 - start1
    print("Time elapsed for CSVDataTable is ", elapsed1, "seconds.")

    start2 = time.time()
    r2 = rdb_tbl.update_by_template(tmp, value)
    end2 = time.time()
    elapsed2 = end2 - start2
    print("Time elapsed for RDBDataTable is ", elapsed2, "seconds.")

    print("******************** " + "end test_update_by_template" + " ********************\n")


def test_insert():
    print("\n******************** " + "test_insert" + " ********************")

    print("Compare time elapsed for insert.")
    csv_tbl = CSVDataTable(table_name, csv_connect_info, key_columns)
    rdb_tbl = RDBDataTable(table_name, rdb_connect_info, key_columns)
    row = {
        "playerID": "coms4111f19",
        "birthYear": "2000",
        "birthMonth": "1",
        "birthDay": "1"
    }

    start1 = time.time()
    csv_tbl.insert(row)
    end1 = time.time()
    elapsed1 = end1 - start1
    print("Time elapsed for CSVDataTable is ", elapsed1, "seconds.")

    start2 = time.time()
    rdb_tbl.insert(row)
    end2 = time.time()
    elapsed2 = end2 - start2
    print("Time elapsed for RDBDataTable is ", elapsed2, "seconds.")

    print("******************** " + "end test_insert" + " ********************\n")


if __name__ == '__main__':
    print("\nCompare elapsed time between CSVDataTable and RDBDataTable.")
    test_init()
    test_find_by_primary_key()
    test_find_by_template()
    test_delete_by_key()
    test_delete_by_template()
    test_update_by_key()
    test_update_by_template()
    test_insert()
