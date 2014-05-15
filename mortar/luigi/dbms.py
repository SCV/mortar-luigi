# Copyright (c) 2013 Mortar Data
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

import abc

import luigi
import logging
from mortar.luigi import target_factory

import psycopg2
import psycopg2.errorcodes
import psycopg2.extensions
import mysql.connector

logger = logging.getLogger('luigi-interface')


class DMBSTask(luigi.Task):

    """
    Class for tasks interacting with SQL Databases.
    """

    @abc.abstractmethod
    def table_name(self):
        """
        Name of the table
        """
        raise RuntimeError("Must provide a table name")


    @abc.abstractmethod
    def output_token(self):
        """
        Token to be written out on completion of the task.
        """
        raise RuntimeError("Must provide an output token")

    def output(self):
        return self.output_token()

    conn = None

    def get_connection(self):
        """
        DatabaseConnection
        """
        raise RuntimeError("Must provide an output token")



class CreateDBMSTable(DMBSTask):

    def primary_key(self):
        """
        List of primary keys
        """
        raise RuntimeError("Must provide a primary key")

    def field_string(self):
        """
        String enumerating all fields, including primary keys
        e.g.: 'num integer, data varchar'
        """
        raise RuntimeError("Must provide a field string")

    def run(self):
        connection = self.get_connection()
        cur = connection.cursor()
        table_query = self.create_table_query()
        cur.execute(table_query)
        connection.commit()
        cur.close()
        connection.close()
        target_factory.write_file(self.output_token())

    def create_table_query(self):
        primary_keys = ','.join(self.primary_key())
        return 'CREATE TABLE %s(%s, PRIMARY KEY (%s));' % (self.table_name(), self.field_string(), primary_keys)


class SanityTestDBMSTable(DMBSTask):
    """
    General check that the contents of a MongoDB collection exist and contain sentinel ids.
    """

    # number of entries required to be in the collection
    min_total_results = luigi.IntParameter(100)

    # when testing total entries, require that these field names not be null
    non_null_fields = luigi.Parameter([])

    # number of results required to be returned for each primary key
    result_length = luigi.IntParameter(5)

    # when testing specific ids, how many are allowed to fail
    failure_threshold = luigi.IntParameter(2)

    @abc.abstractmethod
    def id_field(self):
        """
        Name of the id field
        """
        raise RuntimeError("Must provide an id field")

    @abc.abstractmethod
    def ids(self):
        """
        Sentinel ids to check
        """
        return RuntimeError("Must provide list of ids to sanity test")

    def run(self):
        """
        Run sanity check.
        """

        cur = self.get_connection().cursor()
        overall_query = self.create_overall_query()
        cur.execute(overall_query)
        rows = cur.fetchall()

        if len(rows) < self.min_total_results:
            exception_string = 'Sanity check failed: only found %s / %s expected results in collection %s' % \
                (len(rows), self.min_total_results, self.table_name())
            logger.warn(exception_string)
            raise DBMSTaskException(exception_string)

        # do a check on specific ids
        self._sanity_check_ids()

        cur.close()
        self.get_connection().close()
        # write token to note completion
        target_factory.write_file(self.output_token())

    def create_overall_query(self):
        where_clause = ' AND '.join(['%s IS NOT NULL' % field for field in self.non_null_fields])
        limit_clause = ' LIMIT %s ' % self.min_total_results
        if where_clause:
            return 'SELECT * FROM %s WHERE %s %s' % (self.table_name(), where_clause, limit_clause)
        return 'SELECT * FROM %s %s' % (self.table_name(), limit_clause)

    def create_id_query(self, id):
        return 'SELECT * FROM %s WHERE %s = \'%s\'' % (self.table_name(), self.id_field(), id)

    def _sanity_check_ids(self):
        failure_count = 0
        cur = self.get_connection().cursor()
        for id in self.ids():
            id_query = self.create_id_query(id)
            cur.execute(id_query)
            rows = cur.fetchall()
            num_results = len(rows)
            if num_results < self.result_length:
                failure_count += 1
                logger.info("Id %s only returned %s results." % (id, num_results))
        if failure_count > self.failure_threshold:
            exception_string = 'Sanity check failed: %s ids in %s failed to return sufficient results' % \
                        (failure_count, self.collection_name())
            logger.warn(exception_string)
            raise DBMSTaskException(exception_string)

class CreatePostgresTable(CreateDBMSTable):

    def get_connection(self):
        if not self.conn:
            dbname = luigi.configuration.get_config().get('postgres', 'dbname')
            user = luigi.configuration.get_config().get('postgres', 'user')
            host = luigi.configuration.get_config().get('postgres', 'host')
            password = luigi.configuration.get_config().get('postgres', 'password')
            port = luigi.configuration.get_config().get('postgres', 'port')

            try:
                self.conn = psycopg2.connect(dbname=dbname, user=user, host=host, password=password, port=port)
            except:
                raise DBMSTaskException("Unable to connect to database %s" % dbname)
        return self.conn

class CreateMySQLTable(CreateDBMSTable):

    def get_connection(self):
        if not self.conn:
            dbname = luigi.configuration.get_config().get('mysql', 'dbname')
            user = luigi.configuration.get_config().get('mysql', 'user')
            host = luigi.configuration.get_config().get('mysql', 'host')
            password = luigi.configuration.get_config().get('mysql', 'password')
            port = luigi.configuration.get_config().get('mysql', 'port')

            try:
                self.conn = mysql.connector.connect(database=dbname, user=user, host=host, password=password, port=port)
            except:
                raise DBMSTaskException("Unable to connect to database %s" % dbname)
        return self.conn

class SanityTestPostgresTable(SanityTestDBMSTable):

    def get_connection(self):
        if not self.conn:
            dbname = luigi.configuration.get_config().get('postgres', 'dbname')
            user = luigi.configuration.get_config().get('postgres', 'user')
            host = luigi.configuration.get_config().get('postgres', 'host')
            password = luigi.configuration.get_config().get('postgres', 'password')
            port = luigi.configuration.get_config().get('postgres', 'port')

            try:
                self.conn = psycopg2.connect(dbname=dbname, user=user, host=host, password=password, port=port)
            except:
                raise DBMSTaskException("Unable to connect to database %s" % dbname)
        return self.conn

class SanityTestMySQLTable(SanityTestDBMSTable):

    def get_connection(self):
        if not self.conn:
            dbname = luigi.configuration.get_config().get('mysql', 'dbname')
            user = luigi.configuration.get_config().get('mysql', 'user')
            host = luigi.configuration.get_config().get('mysql', 'host')
            password = luigi.configuration.get_config().get('mysql', 'password')
            port = luigi.configuration.get_config().get('mysql', 'port')

            try:
                self.conn = mysql.connector.connect(database=dbname, user=user, host=host, password=password, port=port)
            except:
                raise DBMSTaskException("Unable to connect to database %s" % dbname)
        return self.conn

class DBMSTaskException(Exception):
    """
    Exception thrown by MongoDBTasks
    """
    pass
