#!venv/bin/python3

import csv
import json
import urllib
import sqlite3
import requests
import socket
socket.setdefaulttimeout(None)

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
        id_matching_filepath = self.fetch_id_matching()
        self.transactions_filepath = self.fetch_transactions()
        self.connection = sqlite3.connect(id_matching_filepath)
        self.cursor = self.connection.cursor()
        self._initialize_table()
        self._import_transactions()
        self._join_tables()
        self.connection.commit()

    def _get_mapped_id(self, uid=None):
        s = requests.Session()
        url = 'http://gum.criteo.com/sync?c={client}'.format(client=gum_client_id)
        if uid:
            cookies = {'uid':uid}
            r = s.get(url, cookies=cookies)
        else:
            for _ in range(2): # You need to sync twice to actually get an ID
                r = s.get(url)
        return r.text


    def _insert_values(self, values):
        sql = "INSERT INTO {tablename} ({columnnames}) VALUES ({qmarks});"
        sql = sql.format(
            tablename=self.transaction_table,
            columnnames=", ".join(self.column_names),
            qmarks=", ".join(['?' for _ in values]),
        )
        self.cursor.execute(sql, tuple(values))

    # Download ID matching data or use local file
    def fetch_id_matching(self):
        path = self.configuration['id_matching_url']
        try:
            local_filename, headers = urllib.request.urlretrieve(path)
            return local_filename
        except ValueError:
            return path

    # Download transactions data or use local file
    def fetch_transactions(self):
        path = self.configuration['transactions_url']
        try:
            local_filename, headers = urllib.request.urlretrieve(path)
            return local_filename
        except ValueError:
            return path

    # Clean the database before import
    def _initialize_table(self):
        # Initialize table
        sql = "DROP TABLE IF EXISTS {tablename}"
        sql = sql.format(tablename=self.transaction_table)
        self.cursor.execute(sql)
        sql = "CREATE TABLE {tablename} ({columnnames});"
        sql = sql.format(tablename=self.transaction_table, columnnames=", ".join(self.column_names))
        self.cursor.execute(sql)
    
    # Import the transactions from a file into the sqlite db
    def _import_transactions(self):
        with open(self.transactions_filepath,'r') as transactions_file: 
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
                    self._insert_values(values)
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
                    self._insert_values(values)
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
                        self._insert_values(values)
                except KeyError:
                    pass

    # Join transactions and ID matching
    def _join_tables(self):
        sql = """
        SELECT data.CriteoId, transactions.transaction_id, transactions.product_id, transactions.quantity, transactions.price
        FROM transactions
        LEFT JOIN data
        ON transactions.external_user_id=data.BusinessID;
        """
        self.transactions = []
        for row in self.cursor.execute(sql):
            for transaction in self.transactions:
                if transaction.trid == row[1] and transaction.userid == row[0]:
                    transaction.products.append(Product(row[2],row[3],row[4]))
                    break
            else:
                transaction = Transaction(row[1], row[0])
                transaction.products.append(Product(row[2],row[3],row[4]))
                self.transactions.append(transaction)

    # Send list of events to Criteo servers
    def send_events(self):
        for transaction in self.transactions:
            products = []
            for product in transaction.products:
                products.append(
                    {
                        'id': product.prid,
                        'price': product.price,
                        'quantity': product.quantity,
                    }
                )
            data = {
                "account": self.configuration['partner_id'],
                "id": {
                    "mapped_user_id": transaction.userid,
                    "mapping_key": self.configuration['gum_client_id'],
                },
                "events": [
                    #{ 'event': "setHashedEmail", 'email': row[user_md5].strip() },
                    { 'event': "trackTransaction", 'id': transaction.trid, 'item': products },
                ]
            }
            # Pass data object
            session = requests.Session()
            encoded_data = urllib.parse.quote(json.dumps(data))
            url = 'http://widget.criteo.com/m/event?version=s2s_v0&data={data}'.format(data=encoded_data)
            response = session.get(url)
            print(response.text)




configuration = {
    "gum_client_id": '70', # 70 is GUM ID for 'Local (TS UK)'
    "partner_id": "976",
    "transactions_url": "transactions.csv",
    "transactions_map": {
        "transaction_id": "transactionid",
        "external_user_id": "businessid",
        "user_md5mail": "",
        "product_id": "productid",
        "quantity": "q",
        "price": "p",
    },
    "id_matching_url": "http://hartshorne.org/criteo/xmlbuilder/data/data.db",
    "id_matching_map": {
        "gum_user_id": "CriteoId",
        "external_user_id": "BusinessID",
    },
}

attribution = Attribution(configuration)
attribution.send_events()
