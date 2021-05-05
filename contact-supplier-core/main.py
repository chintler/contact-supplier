import requests
import json
import pandas as pd
import datetime
import traceback
import argparse
import utils
from classes import *


def process(csv_file_path):
    row_limiter = 0
    df = utils.load_suppliers_csv(csv_file_path)
    suppliers = []

    for rownum, row_data in enumerate(df.iterrows()):
        row = row_data[1]
        print(rownum, row['name'])
        supplier = Supplier(name=row['name'], pincode=row['pincode'],
                            contact_num=row['contact'], supplier_type=row['type'])
        suppliers.append(supplier)
        if rownum == row_limiter:
            break

    utils.multiprocess_supplier_messages(suppliers)

    for supp in suppliers:
        for msg in supp.messages[::-1]:
          if msg.event_type == "message":
            if msg.owner:
              print("*"*25, msg.text, msg.timestamp)
            else:
              print(msg.text, msg.timestamp)
          elif msg.event_type == "broadcastMessage":
            print("~"*25, msg.finalText, msg.timestamp)

        print("="*25)
        print("")
        print("")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='lets find some suppliers')
    parser.add_argument('--csv_path', type=str,
                        help='csv containing supplier data')
    args = parser.parse_args()
    print(args.csv_path)
    process(args.csv_path)
