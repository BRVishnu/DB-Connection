import oracledb
import pandas as pd
import os
import logging

if 'loggings' in os.listdir():
    pass
else:
    os.mkdir('loggings')


class DBCONN:

    def __init__(self, username, password, host='DBHDWIN.IN.PROD', service_name='HDWIN.HOMECREDIT.IN', port=1521):
        """

        :param username: provide the username
        :param password: user password

        """
        self.__user = username
        self.__password = password
        self._host = host
        self._service_name = service_name
        self._port = port
        self.dtype_dict = {'number': ['int64', 'int32'],
                           'decimal': ['float64', 'float32'],
                           'string': ['object'],
                           'date': ['datetime64[ns]', 'datetime32[ns]']}

        try:
            log = logging.getLogger("DB_CONN")
            file_handler = logging.FileHandler(os.getcwd() + '\\loggings\\' + 'DB_CONN.txt', mode='a')
            log_format = logging.Formatter('%(name)s : %(asctime)s : %(levelname)s : %(message)s')
            file_handler.setFormatter(log_format)
            log.addHandler(file_handler)
            log.setLevel(logging.INFO)

            self.__log = log
        except Exception as e:
            print("DB CONN logs not created on error: ", e)

        self.__log.info("DataBase Connection object has been created")

    def __str__(self):
        return "Oracle Database Connection class"

    # function to open the database connection

    def open_db_connection(self):
        """
        returns the db_connection object. Can be used for db operations.
        Kindly close the db connection after the operation using '.close()' function
        """
        self.__log.info("called: open_db_connection function")

        try:
            dsn_tns = oracledb.makedsn(host=self._host, port=self._port, service_name=self._service_name)
            conn = oracledb.connect(user=self.__user, password=self.__password, dsn=dsn_tns)
            c = conn.cursor()
            self.__log.info("successful: open_db_connection function")
            return c
        except Exception as e:
            self.__log.error('Error: open_db_connection function: ', e)

    # function for extracting table from oracle database

    def sql_table_output(self, query):
        """
        :param query: The sql query in a string format without the semicolon
        :returns the table based on the query entered
        """

        self.__log.info("called: sql_table_output function")
        try:
            c = self.open_db_connection()
            c.execute(query)
            col_names = [row[0] for row in c.description]
            df = c.fetchall()
            df = pd.DataFrame(df, columns=col_names)
            c.close()
            self.__log.info("successful: sql_table_output function")
            return df
        except Exception as e:
            self.__log.error(f"error: sql_table_output function: {e}")

    # function for creating a sql query to create a table in the db
    def __sql_query_to_create_table(self, data, table_name, columns):
        """
        The function creates a sql query for creating a table in the db
        :param data: data to be inserted into the table
        :param table_name: name of the table that needs to be created
        :param columns: columns of the table in a list format
        :return: returns the sql query
        """
        self.__log.info("called: sql_query_to_create_table function")

        q1 = 'create table ' + table_name + ' ('
        try:
            for i in columns:

                if len(columns) - 1 != columns.index(i):
                    if data[i].dtype in self.dtype_dict['number']:
                        q1 = q1 + i + " number, "
                    elif data[i].dtype in self.dtype_dict['decimal']:
                        q1 = q1 + i + " decimal, "
                    elif data[i].dtype in self.dtype_dict['string']:
                        string_len = max(data[i].apply(lambda x: len(x)))
                        if string_len <= 10:
                            q1 = q1 + i + " varchar(10), "
                        elif string_len <= 20:
                            q1 = q1 + i + " varchar(20), "
                        elif string_len <= 50:
                            q1 = q1 + i + " varchar(50), "
                        elif string_len <= 100:
                            q1 = q1 + i + " varchar(100), "
                        elif string_len > 100:
                            q1 = q1 + i + " varchar(" + str(round(string_len/100, 0)*100) + "), "
                    elif data[i].dtype in self.dtype_dict['date']:
                        q1 = q1 + i + " date, "
                    else:
                        raise Exception("The data type is not captured in the "
                                        "predefined dtype dictionary for column: ", i)

                else:
                    if data[i].dtype in self.dtype_dict['number']:
                        q1 = q1 + i + " number)"
                    elif data[i].dtype in self.dtype_dict['decimal']:
                        q1 = q1 + i + " decimal)"
                    elif data[i].dtype in self.dtype_dict['string']:
                        string_len = max(data[i].apply(lambda x: len(x)))
                        if string_len <= 10:
                            q1 = q1 + i + " varchar(10), "
                        elif string_len <= 20:
                            q1 = q1 + i + " varchar(20), "
                        elif string_len <= 50:
                            q1 = q1 + i + " varchar(50), "
                        elif string_len <= 100:
                            q1 = q1 + i + " varchar(100), "
                        elif string_len > 100:
                            q1 = q1 + i + " varchar(" + str(round(string_len / 100, 0) * 100) + "), "
                    elif data[i].dtype in self.dtype_dict['date']:
                        q1 = q1 + i + " date)"
                    else:
                        raise Exception("The data type is not captured in the "
                                        "predefined dtype dictionary for column: ", i)
        except Exception as e:
            self.__log.error(f"error: sql_query_to_create_table function: {e}")
        return q1

    # function for creating a sql query to insert data to a table
    def __sql_query_for_insert(self, table_name, columns):
        """
        The fn is for creating the sql query to insert data into a table
        :param table_name: name of the table to which data is to be inserted
        :param columns: columns in the table in order and list format
        :return: the sql query
        """
        self.__log.info("called: sql_query_for_insert function")
        try:
            q1 = "insert into " + table_name + "("
            q2 = " values ("
            for i in range(0, len(columns)):
                if i + 1 != len(columns):
                    q1 = q1 + columns[i] + ", "
                    q2 = q2 + ":" + str((i + 1)) + ", "
                else:
                    q1 = q1 + columns[i] + ")"
                    q2 = q2 + ":" + str((i + 1)) + ")"
            return q1 + q2
        except Exception as e:
            self.__log.error(f"error: sql_query_for_insert function: {e}")

    def sql_data_insert(self, data, table_name, columns='optional'):
        """
        The fn is to create and insert data into a table
        :param data: data to be inserted should be provided in a dataframe
        :param table_name: the table into which the data needs to be inserted
        :param columns: column names of the table in list format. If not provided will pick from the table.
        """
        self.__log.info("called: sql_data_insert function")
        try:
            if type(data) != pd.DataFrame:
                raise Exception("Data Should be in DataFrame format")
        except Exception as e:
            self.__log.error(f"error: sql_data_insert function: {e}")
        else:
            # data in dataFrame format is converted to list
            data_insert = data.values.tolist()

            try:
                if columns == 'optional':
                    columns = data.columns.tolist()
                elif type(columns) != list:
                    raise "Columns should be in List Format"
            except Exception as e:
                self.__log.error(f"error: sql_data_insert function: {e}")

            try:
                table_create_query = self.__sql_query_to_create_table(data=data, table_name=table_name, columns=columns)
                insert_query = self.__sql_query_for_insert(table_name=table_name, columns=columns)
                c = self.open_db_connection()
                print(table_create_query)
                c.execute(table_create_query)
                c.executemany(insert_query, data_insert)
                c.execute('commit')
                c.close()
            except Exception as e:
                self.__log.error(f"error: sql_data_insert function: {e}")
            else:
                self.__log.info("successful: sql_data_insert function")