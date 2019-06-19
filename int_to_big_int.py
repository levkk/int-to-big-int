#!/usr/bin/python3

import psycopg2
import argparse

BATCH_SIZE = 1000
DEBUG = os.environ.get('DEBUG', 'False') == 'True'

def __execute(cursor, query):
    '''Execute the query using the cursor.
    Helps with debugging.

    Arguments:
        - cursor: psycopg2.cursor
        - query: str
    '''

    if DEBUG:
        print(query)

    cursor.execute(query)


class IntToBigInt():
    '''Convert INT columns to BIGINT without downtime.'''

    def __init__(self, table, column):
        '''
        Arguments:
            - table: str Target table
            - column: str Target column
        '''

        self.table = table
        self.column = column
        self.temp_column = f'{column}_tmp'
        self.conn = psycopg2.connection(os.environ.get('DATABASE_URL'))


    def setup_trigger(self):
        '''Setup the backfilling real-time trigger.
        All new entries will have the new value saved
        in the temp column.'''
        
        self.drop_trigger()

        create_function = f'''
        CREATE FUNCTION automatic_insert_new_values() RETURNS trigger AS $automatic_insert_new_values$
            BEGIN
                NEW."{self.temp_column}" = NEW."{self.column}";

                RETURN NEW;
            END;
        $automatic_insert_new_values$ LANGUAGE plpgsql;
        '''
        create_trigger = f'''
            CREATE TRIGGER automatic_insert_new_values_trigger BEFORE INSERT OR UPDATE ON "{self.table}"
            FOR EACH ROW EXECUTE PROCEDURE automatic_insert_new_values();
        '''

        cursor = self.conn.cursor()

        __execute(cursor, create_function)
        __execute(cursor, create_trigger)

        self.conn.commit()


    def drop_trigger(self):
        '''Drop the backfilling trigger.'''

        drop_function = '''
            DROP FUNCTION IF EXISTS automatic_insert_new_values();
        '''
        drop_trigger = '''
            DROP TRIGGER IF EXISTS automatic_insert_new_values_trigger;
        '''

        cursor = self.conn.cursor()

        __execute(cursor, drop_trigger)
        __execute(cursor, drop_function)

        self.conn.commit()


    def create_temp_column(self):
        '''Create a temporary column with the 
        desired datatype to backfill it.'''

        query = f'''
            ALTER TABLE "{self.table}"
            ADD COLUMN "{self.temp_column}" BIGINT NULL;
        '''

        cursor = self.conn.cursor()

        __execute(cursor, query)

        self.conn.commit()


    def min_max_column_values(self):
        '''Get the current MIN(column) and MAX(column)
        from the table for backfilling.'''

        cursor = self.conn.cursor()

        min_query = f'SELECT MIN("{self.column}") FROM "{self.table}";'
        max_query = f'SELECT MAX("{self.column}") FROM "{self.table}";'

        __execute(cursor, min_query)
        min_column_value = cursor.fetchall()[0][0]

        __execute(cursor, max_query)
        max_column_value = cursor.fetchall()[0][0]

        self.conn.rollback() # Noop on selects

        return min_column_value, max_column_value


    def backfill(self):
        '''Execute the backfill in batches.'''

        min_column_value, max_column_value = self.min_max_column_values()
        cursor = self.conn.cursor()

        while min_column_value < max_column_value:
            query = f'''
                UPDATE "{self.table}"
                SET "{self.temp_column}" = "{self.column}"
                WHERE "{self.column}" >= {min_column_value}
                AND "{self.column}" <= {max_column_value};
            ''' # Little overlap, no biggie

            __execute(cursor, query)
            cursor.commit() # TBD if I want to do this here

            min_column_value += BATCH_SIZE


    def switch_columns(self):
        '''Perform the switch of temp and column in a 
        single transaction while locking the table.

        This will be very quick.
        '''

        temp_column_name = f'{self.column}_rename_tmp'

        lock_table = f'''
            LOCK TABLE "{self.table}" IN ACCESS EXCLUSIVE MODE;
        '''
        one = f'''
            ALTER TABLE "{self.table}"
            RENAME COLUMN "{self.column}" TO "{temp_column_name}";
        '''
        two = f'''
            ALTER TABLE "{self.table}"
            RENAME COLUMN "{self.temp_column}" TO "{self.column}";
        '''
        # https://stackoverflow.com/a/15700185/10321822
        three = f'''
            ALTER TABLE "{self.table}"
            DROP COLUMN "{temp_column_name}";
        '''

        cursor = self.conn.cursor()

        __execute(cursor, lock_table)
        self.drop_trigger()
        __execute(cursor, one)
        __execute(cursor, two)
        __execute(cursor, three)

        self.conn.commit() # Will release lock
        

    def run(self):
        '''Entrypoint.'''

        self.create_temp_column()
        self.create_trigger()
        self.backfill()
        self.switch_columns()
