import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
	"""
	- Creates and connects to the sparkifydb
	- Returns the connection and cursor to sparkifydb
	"""

	# connect to default database
	try:
		conn = psycopg2.connect(user = "postgres",
								password = "741100",
								dbname = "mydb",
								host = "127.0.0.1")
		conn.set_session(autocommit = True)
		cur = conn.cursor()

		# create sparkify database with UTF8 encoding
		cur.execute("DROP DATABASE IF EXISTS sparkifydb")
		cur.execute("CREATE DATABASE sparkifydb \
						WITH ENCODING 'utf8' TEMPLATE template0")
	except psycopg2.Error as e:
		print(e)

	conn.close()

	# connect to sparkifydb
	try:
		conn = psycopg2.connect(user = "postgres",
							password = "741100",
							dbname = "sparkifydb",
							host = "127.0.0.1")
		cur = conn.cursor()
	except psycopg2.Error as e:
		print(e)

	return cur, conn


def drop_tables(cur, conn):
	"""
	Drops each table in drop_table_queries
	"""
	for query in drop_table_queries:
		try:
			cur.execute(query)
			conn.commit()
		except psycopg2.Error as e:
			print("Error")
			print(e)

def create_tables(cur, conn):
	for query in create_table_queries:
		try:
			cur.execute(query)
			conn.commit()
		except psycopg2.Error as e:
			print("Error")
			print(e)
			

def main():
	cur, conn = create_database()

	drop_tables(cur, conn)
	create_tables(cur, conn)

	conn.close()


if __name__ == '__main__':
	main()