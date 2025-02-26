import threading
from sql_pgrm.log_class import Logger
import pandas as pd
import configparser
import psycopg2

log = Logger()


class SqlOperation:

    def read_config(self, filename='app.properties'):
        config = configparser.ConfigParser()
        config.read(filename)
        db_config = {
                    'host': config.get('db', 'host'),
                    'user': config.get('db', 'user'),
                    'password': config.get('db', 'password'),
                    'database': config.get('db', 'dbname'),
                    'port': int(config.get('db', 'port'))
        }
        return db_config

    def create_connection(self):
        try:
            config = self.read_config()
            conn = psycopg2.connect(**config)
            log.info('Postgres db connection established successfully.')
            return conn
        except psycopg2.OperationalError as e:
            log.error(f'Database connection error: {e}')
            return None

    def create_table(self, name):
        """Creates a table if it does not exist."""
        query = f'''
                 CREATE TABLE IF NOT EXISTS {name} (
                     Id SERIAL PRIMARY KEY,
                     Customer_Name Varchar(50),
                     Product Varchar(50),
                     Quantity INT,
                     MRP INT,
                     Date TIMESTAMP
                 )
                 '''
        conn = self.create_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query)
            conn.commit()
            log.info(f"Table ``{name}`` created successfully in Postgres.")
        except Exception as e:
            log.error(f"Error creating table: {e}")
        finally:
            conn.close()

    def __insert_batch_data(self, dbname, batch_data):
        """Inserts data from DataFrame into MySQL table."""
        query = (
            '''
            INSERT INTO Bills 
            (Customer_Name, Product, MRP, Quantity, Date) VALUES (%s, %s, %s, %s, %s)
            ''')
        conn = self.create_connection()
        if conn:
            try:
                with conn.cursor() as cursor:
                    cursor.executemany(query, batch_data)
                conn.commit()
                log.info(f"Inserted batch of {len(batch_data)} records successfully.")
            except Exception as e:
                log.error(f"Error inserting batch: {e}")
            finally:
                conn.close()

    def insert_data_multithreaded(self, df, db_name, num_threads=2):
        """Splits data into batches and inserts using multiple thread_list."""
        batch_size = len(df) // num_threads  # Divide data into equal batches
        thread_list = []

        for i in range(num_threads):
            start_idx = i * batch_size
            end_idx = None if i == num_threads - 1 else (i + 1) * batch_size  # Handle last batch
            data_batch = [
                # (row.name, row.age, row.city, self.parse_date(row.dob))  # Parse data before inserting
                tuple(row)
                for row in df.iloc[start_idx:end_idx].itertuples(index=False, name=None)
            ]
            thread = threading.Thread(target=self.__insert_batch_data, args=(db_name, data_batch,))
            thread_list.append(thread)
            thread.start()  # Start the thread

        for thread in thread_list:
            thread.join()  # Wait for all thread_list to finish

    def fetch_limited_data(self, query, output_file):
        """
        Fetches a limited number of records from MySQL and stores them in a CSV file.

        :param db_config: Dictionary with database connection parameters
        :param query: SQL query to fetch limited data
        :param output_file: File path to save the data
        """
        # Create connection
        conn = self.create_connection()
        cursor = conn.cursor()
        try:
            # Execute query
            cursor.execute(query)
            rows = cursor.fetchall()

            # Get column names
            columns = [desc[0] for desc in cursor.description]

            # Convert to DataFrame and save as CSV
            df = pd.DataFrame(rows, columns=columns)
            df.to_csv(output_file, index=False)

            log.info(f"{len(df)} Data fetched from DB and saved to {output_file}")

        except Exception as e:
            log.error(f"MySQL Error: {e}")

        finally:
            # Close connection
            cursor.close()
            conn.close()

    def remove_duplicates_sql(self):
        multi_query = '''
        create table temp as
        select distinct * from retail_market_sales;
        drop table retail_market_sales;
        rename table temp to retail_market_;
        '''
        try:
            with self.create_connection() as conn:
                with conn.cursor() as cursor:
                    for query in multi_query.strip().split(";"):  # Split queries by ';'
                        if query.strip():  # Ignore empty queries
                            cursor.execute(query)
                conn.commit()  # Commit changes after execution
            print("All queries executed successfully.")
        except Exception as e:
            print(f"Error executing queries: {e}")
