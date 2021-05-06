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
    '''

    The columns are 
     - conversation_id(same for the entire thread)
     - id - message_id
     - assi_id - assigned_id
     - is_bot -> Bot if it is bot else Supplier
     - message -> text
    '''
    columns = ['cid','id','assi_id','timestamp','is_bot','message','is_reply','thread_id']
    #create an empty dataframe
    supplier_df = pd.DataFrame(columns = columns)
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
    #print('^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^')

    for supp in suppliers:
        for msg in supp.messages[::-1]:
            '''
            Find if the message is end of conversation(by checking for 'actor' attribute)
            '''
            if hasattr(msg,'actor'):
                cid = msg.conversationId
                id_ = msg.id
                assi_id = None
                timestamp = None
                is_bot = 'Bot'
                message = msg.eventDescription
                is_reply = False
                thread_id = None
            # for normal messages, get the attributes for dataframe
            else:
                cid = msg.conversationId
                id_ = msg.id
                assi_id = msg.assignedId
                timestamp = msg.timestamp
                is_bot = msg.is_bot
                message = msg.text
                is_reply = False
                thread_id = None
            #append the row to the dataframe
            supplier_df = supplier_df.append({'cid':cid,'id':id_,'assi_id': assi_id,'timestamp':timestamp,'is_bot':is_bot,'message':message,'is_reply':is_reply,'thread_id':thread_id},ignore_index  = True)

    print(supplier_df)
    #save it to a csv file
    supplier_df.to_csv('sample1.csv')
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='lets find some suppliers')
    parser.add_argument('--csv_path', type=str,
                      help='csv containing supplier data')
    args = parser.parse_args()
    print(args.csv_path)
    process(args.csv_path)
