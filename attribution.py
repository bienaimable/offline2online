#Source: Hashed email; Product1;q1;p1;p2;q2;p2;...
#Use GUM to get a new Criteo ID
#Send the full transaction with Criteo ID and hashed email

#import requests
import csv
import json
import urllib
import sqlite3

gum_client_id = '70' # GUM ID for 'Local (TS UK)'

# Define column names
partner_id = 'partnerid'
user_md5 = 'usermd5'
user_id = 'usercriteoid'
product_id = 'productid'
quantity = 'q'
price = 'p'

configuration = {
    "gum_client_id": '70', # GUM ID for 'Local (TS UK)'
    "partner_id": "976",
    "transactions_url": "http://",
    "transactions_map": {
        "gum_user_id": "",
        "external_user_id": "",
        "user_md5mail": "usermd5",
        "product_id": "productid",
        "quantity": "q",
        "price": "p",
    },
    "id_matching_url": "http://",
    "id_matching_map": {
        "gum_user_id": "CriteoId",
        "external_user_id": "BusinessID",
    },
}

###
# Download ID matching data

###
# Download transactions data

###
# Import transactions data to database
connection = sqlite3.connect('data.db')
cursor = connection.cursor()

transaction_table = "transactions"
column_names = []
column_names.append('transaction_id')
column_names.append('external_user_id')
column_names.append('product_id')
column_names.append('quantity')
column_names.append('price')

sql = "DROP TABLE IF EXISTS {tablename}"
sql = sql.format(tablename=transaction_table)
cursor.execute(sql)
sql = "CREATE TABLE {tablename} ({columnnames});"
sql = sql.format(tablename=transaction_table, columnnames=", ".join(column_names))
cursor.execute(sql)

with open('transactions.csv','r') as transactions_file: 
    transactions = csv.DictReader( #uses first line for column headings
        transactions_file,
        delimiter=',',
        quoting=csv.QUOTE_NONE
    ) 
    for transaction in transactions:
        sql = "INSERT INTO {tablename} ({columnnames}) VALUES ({values});"
        sql = sql.format(
            tablename=transaction_table,
            columnnames=", ".join(column_names),
            values=", ".join(column_names),
        )
        cursor.execute(sql)
connection.commit()


sql = "INSERT INTO {tablename} ({columnnames}) VALUES ({values});"
cursor.executemany(sql, to_db)
connection.commit()

# Join transactions and ID matching

# Send list of events to Criteo servers




def get_new_id():
    s = requests.Session()
    for _ in range(2):
        r = s.get('http://gum.criteo.com/sync?c={client}&a=1'.format(client=gum_client_id))
    return r.text

## Import from CSV
#with open('input.csv') as csvfile:
#    reader = csv.DictReader(csvfile)
#    # Loop through rows
#    for row in reader:
#        # Build product list
#        products = []
#        product_number = 1
#        try:
#            while True:
#                products.append(
#                        {
#                            'id': row[product_id + str(product_number)].strip(),
#                            'price': row[price + str(product_number)].strip(),
#                            'quantity': row[quantity + str(product_number)].strip()
#                        }
#                )
#                product_number = product_number + 1
#        except(KeyError):
#            pass
#        # Build data object
#        if row[user_id].strip():
#            mapped_user_id = row[user_id].strip()
#        else:
#            mapped_user_id = get_new_id()
#        data = {
#            "account": row[partner_id].strip(),
#            "id": {
#                "mapped_user_id": mapped_user_id,
#                "mapping_key": gum_client_id
#            },
#            "events": [
#                { 'event': "setHashedEmail", 'email': row[user_md5].strip() },
#                { 'event': "trackTransaction", 'item': products }
#            ]
#        }
#        # Pass data object
#        s = requests.Session()
#        encoded_data = urllib.parse.quote(json.dumps(data))
#        url = 'http://widget.criteo.com/m/event?version=s2s_v0&data={data}'.format(data=encoded_data)
#        s = s.get(url)
#


