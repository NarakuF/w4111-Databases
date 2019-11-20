# W4111_F19_HW2
Implementation for homework 2 - Overview Database Applications

## Special Design Decisions
1. I use the second approach to get databases and tables as TA meantioned on Piazza.
2. I add a custom method in data_table_adaptor, get_db_tables since it is a protected variable in that module.
3. In the extra credit part, if only send limit but not offset, I set the offset be 0 and there is no previous, only next.

## TODO
1. Did not get primary key order.
2. Did not test compound key.
