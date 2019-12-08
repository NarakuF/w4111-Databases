import CSVCatalog
import json

app_csv = './Appearances.csv'
batting_csv = './Batting.csv'
people_csv = './People.csv'


def t1():
    c_cat = CSVCatalog.CSVCatalog()
    print("T1: result = \n", c_cat)


def t2():
    c_cat = CSVCatalog.CSVCatalog()
    res = c_cat.create_table('People', people_csv)
    print("\nT2: result = \n", res)


def t3():
    c_cat = CSVCatalog.CSVCatalog()
    res = c_cat.get_table('People')
    print("\nT3: result = \n", res)


def t4():
    c_cat = CSVCatalog.CSVCatalog()
    res = c_cat.create_table('Batting', batting_csv)
    print("\nT4: result = \n", res)
    res = c_cat.get_table('People')
    print("\nT4: result = \n", res)


def t5():
    col = CSVCatalog.ColumnDefinition('playerID', 'text', True)
    print("\nT5: result = \n", col)


def t6():
    idx = CSVCatalog.IndexDefinition('People', 'abc', 'PRIMARY', ['playerID', 'TeamID', 'yearID', 'stint'])
    print("\nT6: result = \n", idx)


def t7():
    print('\nT7: test for column operations\n')
    c_cat = CSVCatalog.CSVCatalog()
    t = c_cat.get_table('People')
    print('Before adding: ', t.get_column('playerID'))
    col = CSVCatalog.ColumnDefinition('playerID', 'text', False)
    t.add_column_definition(col)
    print('After adding: ', t.get_column('playerID'))
    t.drop_column_definition(col.column_name)
    print('After dropping: ', t.get_column('playerID'))
    try:
        t.drop_column_definition(col.column_name)
        print('Try dropping twice\n', t.get_column('playerID'))
        print('Wrong. Drop twice.')
    except Exception as e:
        print('Correct. Cannot drop twice.')

    col = CSVCatalog.ColumnDefinition('playerID', 'text', False)
    t.add_column_definition(col)
    col = CSVCatalog.ColumnDefinition('teamID', 'text', False)
    t.add_column_definition(col)
    col = CSVCatalog.ColumnDefinition('yearID', 'text', False)
    t.add_column_definition(col)


def t8():
    c_cat = CSVCatalog.CSVCatalog()
    t = c_cat.get_table('People')
    col = t.load_columns()
    print("\nT8: result = \n")
    for c in col:
        print(c)


if __name__ == '__main__':
    t1()
    t2()
    t3()
    t4()
    t5()
    t6()
    t7()
    t8()

