#!/usr/bin/python3

import csv
import json
import urllib
import sqlite3
#import requests
class Transaction():
    def __init__(self, trid, userid):
        self.trid = trid
        self.userid = userid
        self.products = []

class Product():
    def __init__(self, prid, quantity, price):
        self.prid = prid
        self.quantity = quantity
        self.price = price


class Attribution:
    def __init__(self, configuration):
        self.configuration = configuration
        self.transaction_table = "transactions"
        self.column_names = [
            'transaction_id',
            'external_user_id',
            'product_id',
            'quantity',
            'price',
        ]

    def _get_new_id(self):
        s = requests.Session()
        for _ in range(2):
            r = s.get('http://gum.criteo.com/sync?c={client}&a=1'.format(client=gum_client_id))
        return r.text


    def _insert_values(self, cursor, values):
        sql = "INSERT INTO {tablename} ({columnnames}) VALUES ({qmarks});"
        sql = sql.format(
            tablename=self.transaction_table,
            columnnames=", ".join(self.column_names),
            qmarks=", ".join(['?' for _ in values]),
        )
        cursor.execute(sql, tuple(values))

    # Download ID matching data
    def download_id_matching(self):
        pass

    # Download transactions data
    def download_transactions(self):
        pass

    # Clean the database before import
    def _initialize_table(self, cursor):
        # Initialize table
        sql = "DROP TABLE IF EXISTS {tablename}"
        sql = sql.format(tablename=self.transaction_table)
        cursor.execute(sql)
        sql = "CREATE TABLE {tablename} ({columnnames});"
        sql = sql.format(tablename=self.transaction_table, columnnames=", ".join(self.column_names))
        cursor.execute(sql)
    
    # Import the transactions from a file into the sqlite db
    def _import_transactions(self, cursor):
        with open('transactions.csv','r') as transactions_file: 
            transactions = csv.DictReader( #uses first line for column headings
                transactions_file,
                delimiter=',',
                quoting=csv.QUOTE_NONE
            ) 
            for transaction in transactions:
                # Remove some whitespace from the transactions
                transaction = { k.strip():transaction[k].strip() for k in transaction}
                mapping = self.configuration["transactions_map"]
                external_user_id_map = mapping["external_user_id"]
                if external_user_id_map:
                    external_user_id = transaction[external_user_id_map]
                else:
                    external_user_id = ""
                user_md5mail_map = mapping["user_md5mail"]
                if user_md5mail_map:
                    user_md5mail = transaction[user_md5mail_map]
                transaction_id_map = mapping["transaction_id"]
                if transaction_id_map:
                    transaction_id = transaction[transaction_id_map]
                product_id_map = mapping["product_id"]
                quantity_map = mapping["quantity"]
                price_map = mapping["price"]
                # Let's make some suppositions on the format of the header and test them
                # Try with no suffix
                try:
                    product_id = transaction[product_id_map]
                    quantity = transaction[quantity_map]
                    price = transaction[price_map]
                    values = [
                        transaction_id,
                        external_user_id,
                        product_id,
                        quantity,
                        price,
                    ]
                    self._insert_values(cursor, values)
                except KeyError:
                    pass
                # Try with 0 as a suffix
                try:
                    product_id = transaction[product_id_map+'0']
                    quantity = transaction[quantity_map+'0']
                    price = transaction[price_map+'0']
                    values = [

                        transaction_id,
                        external_user_id,
                        product_id,
                        quantity,
                        price,
                    ]
                    self._insert_values(cursor, values)
                except KeyError:
                    pass
                # Now try with the other numbers
                try:
                    index = 1
                    while True:
                        product_id = transaction[product_id_map+str(index)]
                        quantity = transaction[quantity_map+str(index)]
                        price = transaction[price_map+str(index)]
                        index = index + 1
                        values = [
                            transaction_id,
                            external_user_id,
                            product_id,
                            quantity,
                            price,
                        ]
                        self._insert_values(cursor, values)
                except KeyError:
                    pass

    # Join transactions and ID matching
    def _join_tables(self, cursor):
        sql = """
        SELECT data.CriteoId, transactions.transaction_id, transactions.product_id, transactions.quantity, transactions.price
        FROM transactions
        LEFT JOIN data
        ON transactions.external_user_id=data.BusinessID;
        """
        transactions = []
        for row in cursor.execute(sql):
            for transaction in transactions:
                if transaction.trid == row[1] and transaction.userid == row[0]:
                    transaction.products.append(Product(row[2],row[3],row[4]))
                    break
            else:
                transaction = Transaction(row[1], row[0])
                transaction.products.append(Product(row[2],row[3],row[4]))
                transactions.append(transaction)
        print(transactions)

    # Send list of events to Criteo servers
    def send_events(self):
        connection = sqlite3.connect('data.db')
        cursor = connection.cursor()
        self._initialize_table(cursor)
        self._import_transactions(cursor)
        self._join_tables(cursor)
        connection.commit()
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
        #            mapped_user_id = _get_new_id()
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

configuration = {
    "gum_client_id": '70', # GUM ID for 'Local (TS UK)'
    "partner_id": "976",
    "transactions_url": "http://",
    "transactions_map": {
        "transaction_id": "transactionid",
        "external_user_id": "businessid",
        "user_md5mail": "",
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

attribution = Attribution(configuration)
attribution.send_events()
