# W4111_F19_HW1
Implementation template for homework 1.

## CSVDataTable
1. When initializing a new instance, I check that there are no duplicate field columns, and no duplicate primary key.
2. After updating/deleting/inserting data, I manually check the unique constraint of primary key, using for loop.
3. Manually check for any violation and exception when selecting/updating/deleting/inserting data.
4. All implementations acts same as how database does. For example, if some rows of updating action will
violate unique primary key constraint, then no row would update.
5. After manipulating the data, it is saved in the instance instead of saving to the local CSV file. If I want to
save to file, I need call *save* method.

## RDBDataTable
1. Setup mysql database configuration (connection_info) and save to self._data.
2. Get primary key from db if key columns is *None* (instead of raising exception), the order is same as the order in csv
3. In this way, I can guarantee the primary key in python is consistent with that in database.
4. Compared to CSVDataTable, which I have to manually check exceptions, database will check all constraints
automatically, and in RDBDataTable I only need to *try/except*.
5. Use the helper methods in lecture notes, I assign corresponding sql and args such as template, field_list
to those helper methods and connect to database to fetch the results.
6. Any data changes will reflect in database, if *commit* is *True*, which is default.

## Special Design Decisions
1. Error check: some exception may seems redundant which can only happens in unit test and customize inputs, but I think it is best practice (*correct me it I'm wrong*)
2. If template is *None* when passing in find_by_template, then the method acts as select with no where clause and return all rows in the table.
3. If template is *None* when passing in update/delete_by_template: the methods will do nothing and just return.
4. The different approaches here is because I think it is reasonable to select and view all rows, but it is very dangerous to update or delete all rows, 
which you will lose all data.
5. I compute the elapsed time between CSVDataTable and RDBDataTable, results are in *time.txt*. From the results,
I can see relational Database is much more efficient than CSV, as storage.

## Primary Key
1. I use Fill Lahman Data Model and Initial Subset as reference to select the primary key.
2. Primary key for Batting: (playerID VARCHAR(12), teamID VARCHAR(8), yearID VARCHAR(6), stint INT)
3. Primary key for People: playerID VARCHAR(12)
4. Primary key for Appearances: (playerID VARCHAR(12), teamID VARCHAR(8), yearID VARCHAR(12))
5. In CSVDataTable, I manaully check there is no duplicate primary key in *init* method when I create a new instance to  enforce that constraint.
6. In RDBDataTable, I set the corresponding primary key, NOTNULL and DataType in MySQL Workbench and it can check for that constraint. Python code just need to raise exception if database occurs any exceptions. 