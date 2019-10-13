from src.BaseDataTable import BaseDataTable
import pymysql

import logging

logger = logging.getLogger()


def get_connection(connect_info):
    """

    :param connect_info: A dictionary containing the information necessary to make a PyMySQL connection.
    :return: The connection. May raise an Exception/Error.
    """

    cnx = pymysql.connect(**connect_info)
    return cnx


def run_q(sql, args=None, fetch=True, cur=None, conn=None, commit=True):
    """
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
    """

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
        if commit:
            conn.commit()

    except Exception as e:
        raise e

    return res, data


def template_to_where_clause(template):
    """

    :param template: One of those weird templates
    :return: WHERE clause corresponding to the template.
    """

    if template is None or template == {}:
        return "", None

    args = []
    terms = []

    for k, v in template.items():
        terms.append(" " + k + "=%s ")
        args.append(v)

    w_clause = "AND".join(terms)
    w_clause = " WHERE " + w_clause
    return w_clause, args


def create_select(table_name, template, fields, order_by=None, limit=None, offset=None):
    """
    Produce a select statement: sql string and args.

    :param table_name: Table name: May be fully qualified dbname.tablename or just tablename.
    :param fields: Columns to select (an array of column name)
    :param template: One of Don Ferguson's weird JSON/python dictionary templates.
    :param order_by: Ignore for now.
    :param limit: Ignore for now.
    :param offset: Ignore for now.
    :return: A tuple of the form (sql string, args), where the sql string is a template.
    """

    if fields is None:
        field_list = " * "
    else:
        field_list = " " + ",".join(fields) + " "

    w_clause, args = template_to_where_clause(template)

    sql = "select " + field_list + " from " + table_name + " " + w_clause

    return sql, args


def create_insert(table_name, row):
    """

    :param table_name: A table name, which may be fully qualified.
    :param row: A Python dictionary of the form: { ..., "column_name" : value, ...}
    :return: SQL template string, args for insertion into the template
    """

    result = "Insert into " + table_name + " "
    cols = []
    vals = []

    # This is paranoia. I know that calling keys() and values() should return in matching order,
    # but in the long term only the paranoid survive.
    for k, v in row.items():
        cols.append(k)
        vals.append(v)

    col_clause = "(" + ",".join(cols) + ") "

    no_cols = len(cols)
    terms = ["%s"] * no_cols
    terms = ",".join(terms)
    value_clause = " values (" + terms + ")"

    result += col_clause + value_clause

    return result, vals


def create_update(table_name, new_values, template):
    """

    :param table_name: A table name, which may be fully qualified.
    :param new_values: A dictionary containing cols and the new values.
    :param template: A template to form the where clause.
    :return: An update statement template and args.
    """
    set_terms = []
    args = []

    for k, v in new_values.items():
        set_terms.append(k + "=%s")
        args.append(v)

    s_clause = ",".join(set_terms)
    w_clause, w_args = template_to_where_clause(template)

    # There are %s in the SET clause and the WHERE clause. We need to form
    # the combined args list.
    args.extend(w_args)

    sql = "update " + table_name + " set " + s_clause + " " + w_clause

    return sql, args


def create_delete(table_name, template):
    """

    :param table_name: A table name, which may be fully qualified.
    :param template: A template to form the where clause.
    :return: An delete statement template and args.
    """
    w_clause, args = template_to_where_clause(template)
    sql = "delete from " + table_name + " " + w_clause
    return sql, args


def zip_to_template(fields, values):
    """
    zip fields and values to a dict.
    """
    if fields is None or values is None or len(fields) != len(values):
        raise ValueError("Invalid field/value pairs")
    template = dict(zip(fields, values))
    return template
