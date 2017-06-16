import sqlite3

EVAL_SET = "train"
EVAL_TABLE = "order_products_train"

# EVAL_SET = "prior"
# EVAL_TABLE = "order_products_prior"


def create_table(conn,insert_table):
	c = conn.cursor()
	conn.execute(sql_create_order_transaction_table)

def insert_in_table(conn, order, products_in_order):
	c = conn.cursor()
	c.execute('INSERT INTO order_transaction VALUES (?,?);',(order, products_in_order))

#Connecting to Database
conn = sqlite3.connect('market')
conn.row_factory = lambda cursor, row: row[0]
c = conn.cursor()

#Create new table to have the order number and the product numbers
c.execute("DROP TABLE order_transaction")

sql_create_order_transaction_table = """ CREATE TABLE IF NOT EXISTS order_transaction (
                                        order_id text NOT NULL,
                                        products_in_order text
                                    ); """

create_table(conn,sql_create_order_transaction_table)


# Fetch all the order_ID's for the prior or train data set
c.execute('SELECT ORDER_ID FROM orders WHERE eval_set=? ORDER BY CAST(order_id as INTEGER);', (EVAL_SET,))
orders = c.fetchall()


# Fetch the products in that order and store it in database 
for order in orders:
 	products_in_order = c.execute('SELECT PRODUCT_ID FROM %s WHERE order_id = ?' % (EVAL_TABLE), (str(order),)).fetchall()
	products_in_order = [x.encode('ascii') for x in products_in_order]
	products_in_order_str = ', '.join(products_in_order)
	insert_in_table(conn, order, products_in_order_str)
	print order, products_in_order_str

print "Ran insertion"

conn.commit()
conn.close()



