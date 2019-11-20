import json
import pymysql
import logging
import matplotlib.pyplot as plt
import numpy as np
import scipy.integrate as integrate

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

midterm_conn = pymysql.connect(
    host="localhost",
    user="dbuser",
    password="dbuserdbuser",
    cursorclass=pymysql.cursors.DictCursor)

def run_q(sql, args=None, fetch=True, cur=None, conn=midterm_conn, commit=True):
    '''
    Helper function to run an SQL statement.

    This is a modification that better supports HW1. An RDBDataTable MUST have a connection specified by
    the connection information. This means that this implementation of run_q MUST NOT try to obtain
    a defailt connection.

    :param sql: SQL template with placeholders for parameters. Canno be NULL.
    :param args: Values to pass with statement. May be null.
    :param fetch: Execute a fetch and return data if TRUE.
    :param conn: The database connection to use. This cannot be NULL, unless a cursor is passed.
        DO NOT PASS CURSORS for HW1.
    :param cur: The cursor to use. This is wizard stuff. Do not worry about it for now.
        DO NOT PASS CURSORS for HW1.
    :param commit: This is wizard stuff. Do not worry about it.

    :return: A pair of the form (execute response, fetched data). There will only be fetched data if
        the fetch parameter is True. 'execute response' is the return from the connection.execute, which
        is typically the number of rows effected.
    '''

    cursor_created = False
    connection_created = False

    try:

        if conn is None:
            raise ValueError("In this implementation, conn cannot be None.")

        if cur is None:
            cursor_created = True
            cur = conn.cursor()

        if args is not None:
            log_message = cur.mogrify(sql, args)
        else:
            log_message = sql

        logger.debug("Executing SQL = " + log_message)

        res = cur.execute(sql, args)

        if fetch:
            data = cur.fetchall()
        else:
            data = None

        # Do not ask.
        if commit == True:
            conn.commit()

    except Exception as e:
        raise(e)

    return (res, data)


def create_order(order_info, cnx=midterm_conn):
    """
    Creates (Inserts) the data associated with an order. The order information goes into orders table and each
    and line item/order detail item goes into the ordersdetails table.
    :param order_info: A dictionary. There are top-level elements for the order. There is an orderdetails element
        that is a list of dictionary for the orderdetails elements.
    :param cnx: The database connection to use.
    :return: A tuple of the form (order_insert_count, orderdetals_insert_count), where the values are the number
        of rows inserted into each table.
    """

    # Your code goes here.
    if order_info is None:
        return

    order = {k: v for k, v in order_info.items() if k != 'orderdetails'}
    order_details = order_info.get('orderdetails', None)
    if len(order) == 0 or order_details is None:
        return

    tables = ['classicmodels.orders', 'classicmodels.orderdetails']

    sql = f'SELECT MAX(orderNumber) maxID FROM {tables[0]}'
    _, data = run_q(sql, conn=cnx)
    new_order_id = data[0]['maxID'] + 1

    order_id = order_info.get('orderNumber', None)
    if order_id is None:
        order_id = new_order_id
    else:
        sql = f'SELECT * FROM {tables[0]} WHERE orderNumber = {order_id}'
        res, _ = run_q(sql, conn=cnx)
        if res > 0:
            order_id = new_order_id

    # Create order sql
    order['orderNumber'] = order_id
    order_sql, args = create_insert(tables[0], order)
    order_insert_count, _ = run_q(order_sql, args, conn=cnx)

    # Create order details sql
    order_details_insert_count = 0
    for d in order_details:
        d['orderNumber'] = order_id
        order_details_sql, args = create_insert(tables[1], d)
        res, _ = run_q(order_details_sql, args, conn=cnx)
        order_details_insert_count += res

    return order_insert_count, order_details_insert_count


def create_insert(table_name, new_row):
    sql = "insert into " + table_name + " "

    cols = list(new_row.keys())
    cols = ",".join(cols)
    col_clause = "(" + cols + ") "

    args = list(new_row.values())

    s_stuff = ["%s"] * len(args)
    s_clause = ",".join(s_stuff)
    v_clause = " values(" + s_clause + ")"

    sql += " " + col_clause + " " + v_clause

    return sql, args


def transform_1(x, y, sigma, alpha):
    return (sigma - x + alpha) * (sigma - y + alpha)


def transform_2(x, y, sigma, alpha):
    return (sigma + x - alpha) * (sigma - y + alpha)


def transform_3(x, y, sigma, alpha):
    return (sigma + x - alpha) * (sigma + y - alpha)


if __name__ == '__main__':
    '''
    info = {
            "orderNumber": 10440,
            "orderDate": "1003-05-20",
            "requiredDate": "1003-05-29",
            "shippedDate": "1003-05-22",
            "status": "Shipped",
            "comments": None,
            "customerNumber": 103,
            "orderdetails": [
                {
                    "orderNumber": 10123,
                    "productCode": "S18_1589",
                    "quantityOrdered": 126,
                    "priceEach": "120.71",
                    "orderLineNumber": 2
                },
                {
                    "orderNumber": 10123,
                    "productCode": "S18_2870",
                    "quantityOrdered": 146,
                    "priceEach": "114.84",
                    "orderLineNumber": 3
                },
                {
                    "orderNumber": 10123,
                    "productCode": "S18_3685",
                    "quantityOrdered": 134,
                    "priceEach": "117.26",
                    "orderLineNumber": 4
                },
                {
                    "orderNumber": 10123,
                    "productCode": "S24_1628",
                    "quantityOrdered": 150,
                    "priceEach": "43.27",
                    "orderLineNumber": 1
                }
            ]
        }

    res = create_order(info)
    print(res)'''

    neg =np.array([[-1, 1], [-1, 0], [-1, -1]])
    neg_x, neg_y = neg.T
    pos = np.array([1, 0])
    pos_x, pos_y = pos.T

    plt.axhline(y=0, color='k')
    plt.axvline(x=0, color='k')
    plt.scatter(neg_x, neg_y)
    plt.scatter(pos_x, pos_y, c='r')
    plt.show()


