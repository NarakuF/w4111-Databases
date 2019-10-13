from src.RDBDataTable import RDBDataTable
import logging
import json

# The logging level to use should be an environment variable, not hard coded.
logging.basicConfig(level=logging.ERROR)

# Also, the 'name' of the logger to use should be an environment variable.
logger = logging.getLogger()
logger.setLevel(logging.ERROR)

# Default test settings: test on People table
table_name = "People"
connect_info = {'host': 'localhost',
                'port': 3306,
                'user': 'dbuser',
                'password': 'dbuserdbuser',
                'db': 'lahman2019raw'}
key_columns = ["playerID"]


def test_init_fail():
    print("\n******************** " + "test_init_fail" + " ********************")

    try:
        print("Table is People and key_columns are ['playerID', 'playerID']")
        RDBDataTable(table_name, connect_info, ['playerID', 'playerID'])
    except Exception as e:
        print("Init failed. Exception =", e)
        print("Correct answer.")

    print("******************** " + "end test_init_fail" + " ********************\n")


def test_load_fail():
    print("\n******************** " + "test_load_fail" + " ********************")

    try:
        print("Table is People and key_columns are ['yearID']")
        RDBDataTable(table_name, connect_info, ['yearID'])
    except Exception as e:
        print("Load failed. Exception =", e)
        print("Correct answer.")

    print("******************** " + "end test_load_fail" + " ********************\n")


def test_load_fail2():
    print("\n******************** " + "test_load_fail2" + " ********************")

    try:
        print("Table is People and key_columns are ['birthYear']")
        RDBDataTable(table_name, connect_info, ['birthYear'])
    except Exception as e:
        print("Load failed. Exception =", e)
        print("Correct answer.")

    print("******************** " + "end test_load_fail2" + " ********************\n")


def test_load_good():
    print("\n******************** " + "test_load_good" + " ********************")

    try:
        rdb_tbl = RDBDataTable(table_name, connect_info, key_columns)
        print("Loaded table = \n", rdb_tbl)
        print("Load successfully.")
        print("Correct answer.")
    except Exception as e:
        print("Exception =", e)
        print("Wrong answer.")

    print("******************** " + "end test_load_good" + " ********************\n")


def test_find_by_primary_key_fail():
    print("\n******************** " + "test_find_by_primary_key_fail" + " ********************")

    try:
        rdb_tbl = RDBDataTable(table_name, connect_info, key_columns)
        print("Find by primary key with None key fields")
        rdb_tbl.find_by_primary_key(None)
    except Exception as e:
        print("Exception =", e)
        print("Correct answer.")

    print("******************** " + "end test_find_by_primary_key_fail" + " ********************\n")


def test_find_by_primary_key_good():
    print("\n******************** " + "test_find_by_primary_key_good" + " ********************")

    try:
        rdb_tbl = RDBDataTable(table_name, connect_info, key_columns)
        print("Find by primary key 'aardsda01'")
        res = rdb_tbl.find_by_primary_key(["aardsda01"])
        print("Result =\n", json.dumps(res, indent=2))
        print("Correct answer.")
    except Exception as e:
        print("Exception =", e)
        print("Wrong answer.")

    print("******************** " + "end test_find_by_primary_key_good" + " ********************\n")


def test_find_by_template_fail():
    print("\n******************** " + "test_find_by_template_fail" + " ********************")

    try:
        rdb_tbl = RDBDataTable(table_name, connect_info, key_columns)
        print('Find by template {"birthCountry": "USA", "teamID": "BS1"}')
        tmp = {"birthCountry": "USA", "teamID": "BS1"}
        rdb_tbl.find_by_template(tmp)
    except Exception as e:
        print("Exception =", e)
        print("Correct answer.")

    print("******************** " + "end test_find_by_template_fail" + " ********************\n")


def test_find_by_template_good():
    print("\n******************** " + "test_find_by_template_good" + " ********************")

    try:
        rdb_tbl = RDBDataTable(table_name, connect_info, key_columns)
        print('Find by template {"birthMonth": "9", "birthDay": "22", "birthCountry": "USA", "birthState": "MA"}')
        tmp = {"birthMonth": "9", "birthDay": "22", "birthCountry": "USA", "birthState": "MA"}
        res = rdb_tbl.find_by_template(tmp, ["playerID", "nameFirst", "nameLast", "nameGiven"])
        print("Result =\n", json.dumps(res, indent=2))
        print("Correct answer.")
    except Exception as e:
        print("Exception =", e)
        print("Wrong answer.")

    print("******************** " + "end test_find_by_template_good" + " ********************\n")


def test_delete_by_key_fail():
    print("\n******************** " + "test_delete_by_key_fail" + " ********************")

    try:
        rdb_tbl = RDBDataTable(table_name, connect_info, key_columns)
        print("Delete by primary key with None")
        rdb_tbl.delete_by_key(None)
    except Exception as e:
        print("Exception =", e)
        print("Correct answer.")

    print("******************** " + "end test_delete_by_key_fail" + " ********************\n")


def test_delete_by_key_good():
    print("\n******************** " + "test_delete_by_key_good" + " ********************")

    try:
        rdb_tbl = RDBDataTable(table_name, connect_info, key_columns)
        print("Delete by primary key 'AARDSDA01'")
        res = rdb_tbl.delete_by_key(['AARDSDA01'])
        print("Wrong primary key and nothing to delete. The number of the rows deleted =", res)

        print("\nDelete by primary key 'aardsda01'")
        r1 = rdb_tbl.find_by_primary_key(['aardsda01'])
        print("BEFORE deleting, the row =\n", json.dumps(r1, indent=2))
        print("Deleting...")
        r2 = rdb_tbl.delete_by_key(['aardsda01'])
        print("Delete returned ", r2, "\n")
        r3 = rdb_tbl.find_by_primary_key(['aardsda01'])
        print("AFTER deleting, the row =\n", json.dumps(r3, indent=2))
        print("Correct answer.")
    except Exception as e:
        print("Exception =", e)
        print("Wrong answer.")

    print("******************** " + "end test_delete_by_key_good" + " ********************\n")


def test_delete_by_template_fail():
    print("\n******************** " + "test_delete_by_template_fail" + " ********************")

    try:
        rdb_tbl = RDBDataTable(table_name, connect_info, key_columns)
        print('Delete by template {"birthCountry": "USA", "teamID": "BS1"}')
        tmp = {"birthCountry": "USA", "teamID": "BS1"}
        rdb_tbl.delete_by_template(tmp)
    except Exception as e:
        print("Exception =", e)
        print("Correct answer.")

    print("******************** " + "end test_delete_by_template_fail" + " ********************\n")


def test_delete_by_template_good():
    print("\n******************** " + "test_delete_by_template_good" + " ********************")

    try:
        rdb_tbl = RDBDataTable(table_name, connect_info, key_columns)
        print('Delete by template key {"birthMonth": "9", "birthDay": "22", "birthCountry": "USA",'
              '"birthState": "MA", "deathCountry": "USA"}')
        tmp = {"birthMonth": "9", "birthDay": "22", "birthCountry": "USA",
               "birthState": "MA", "deathCountry": "USA"}
        r1 = rdb_tbl.find_by_template(tmp)
        print('BEFORE deleting, all rows =\n', json.dumps(r1, indent=2))
        print('Deleting...')
        r2 = rdb_tbl.delete_by_template(tmp)
        print("Delete returned ", r2, "\n")
        r3 = rdb_tbl.find_by_template(tmp)
        print('AFTER deleting, all rows =\n', json.dumps(r3, indent=2))
        print("Correct answer.")
    except Exception as e:
        print("Exception =", e)
        print("Wrong answer.")

    print("******************** " + "end test_delete_by_template_good" + " ********************\n")


def test_update_by_key_fail():
    print("\n******************** " + "test_update_by_key_fail" + " ********************")

    try:
        csv_tbl = RDBDataTable(table_name, connect_info, key_columns)
        print('Update by primary key "abadijo01", new value is {"playerID": "howarma01"}')
        csv_tbl.update_by_key(["abadijo01"], {"playerID": "howarma01"})
    except Exception as e:
        print("Exception =", e)
        print("Correct answer.")

    print("******************** " + "end test_update_by_key_fail" + " ********************\n")


def test_update_by_key_good():
    print("\n******************** " + "test_update_by_key_good" + " ********************")

    try:
        rdb_tbl = RDBDataTable(table_name, connect_info, key_columns)
        print('Update by primary key "abadijo01", new value is {"nameFirst": "Jackson", "nameLast": "Copper"}')
        r1 = rdb_tbl.find_by_primary_key(["abadijo01"])
        print("BEFORE updating, the row =\n", json.dumps(r1, indent=2))
        print("Updating...")
        r2 = rdb_tbl.update_by_key(["abadijo01"], {"nameFirst": "Jackson", "nameLast": "Copper"})
        print("Update returned ", r2, "\n")
        r3 = rdb_tbl.find_by_primary_key(["abadijo01"])
        print('AFTER Updating, the row =\n', json.dumps(r3, indent=2))
        print("Correct answer.")
    except Exception as e:
        print("Exception =", e)
        print("Wrong answer.")

    print("******************** " + "end test_update_by_key_good" + " ********************\n")


def test_update_by_template_fail():
    print("\n******************** " + "test_update_by_template_fail" + " ********************")

    try:
        rdb_tbl = RDBDataTable(table_name, connect_info, key_columns)
        print('Update by template {"birthYear": "1995"}, new value is {"playerID": "abbotod01"}')
        rdb_tbl.update_by_template({"birthYear": "1995"}, {"playerID": "abbotod01"})
    except Exception as e:
        print("Exception =", e)
        print("Correct answer.")

    print("******************** " + "end test_update_by_template_fail" + " ********************\n")


def test_update_by_template_good():
    print("\n******************** " + "test_update_by_template_good" + " ********************")

    try:
        rdb_tbl = RDBDataTable(table_name, connect_info, key_columns)
        print('Update by template {"birthYear": "1995", "birthMonth": "11"}, '
              'new value is {"nameFirst": "Jackson", "nameLast": "Copper"}')
        tmp = {"birthYear": "1995", "birthMonth": "11"}
        value = {"nameFirst": "Jackson", "nameLast": "Copper"}
        r1 = rdb_tbl.find_by_template(tmp)
        print("BEFORE updating, all rows =\n", json.dumps(r1, indent=2))
        print("Updating...")
        r2 = rdb_tbl.update_by_template(tmp, value)
        print("Update returned ", r2, "\n")
        r3 = rdb_tbl.find_by_template(tmp)
        print('AFTER Updating, the row =\n', json.dumps(r3, indent=2))
        print("Correct answer.")
    except Exception as e:
        print("Exception =", e)
        print("Wrong answer.")

    print("******************** " + "end test_update_by_template_good" + " ********************\n")


def test_insert_fail():
    print("\n******************** " + "test_insert_fail" + " ********************")

    try:
        bad_row = {
            "playerID": "arroych01",
            "birthYear": "2000",
            "birthMonth": "1",
            "birthDay": "1"
        }
        rdb_tbl = RDBDataTable(table_name, connect_info, key_columns)
        print("Attempting to insert bad row = ", json.dumps(bad_row, indent=2))
        rdb_tbl.insert(bad_row)
    except Exception as e:
        print("Insert failed. Exception = ", e)
        print("Correct answer.")

    print("******************** " + "end test_insert_fail" + " ********************\n")


def test_insert_good():
    print("\n******************** " + "test_insert_good" + " ********************")

    try:
        good_row = {
            "playerID": "coms4111f19",
            "birthYear": "2000",
            "birthMonth": "1",
            "birthDay": "1"
        }
        rdb_tbl = RDBDataTable("People", connect_info, key_columns)
        print("Attempting to insert good row = ", json.dumps(good_row, indent=2))
        rdb_tbl.insert(good_row)
        print("Correct answer.")
    except Exception as e:
        print("Insert failed. Exception = ", e)
        print("Wrong answer.")

    print("******************** " + "end test_insert_good" + " ********************\n")


if __name__ == '__main__':
    # Tests for initialization
    test_init_fail()
    test_load_fail()
    test_load_fail2()
    test_load_good()

    # Tests for methods
    test_find_by_primary_key_fail()
    test_find_by_primary_key_good()

    test_find_by_template_fail()
    test_find_by_template_good()

    test_delete_by_key_fail()
    test_delete_by_key_good()

    test_delete_by_template_fail()
    test_delete_by_template_good()

    test_update_by_key_fail()
    test_update_by_key_good()

    test_update_by_template_fail()
    test_update_by_template_good()

    test_insert_fail()
    test_insert_good()
