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
    '''
    The columns for thread_dataframe are
      - cid - conversation_id
      - msg_id - message_id
      - reply_msg_id -  replied message id
      - msg_txt -> Message sent by the bot
      - reply_msg_txt - reply for the msg from the Supplier
      - entity - what the bot is asking on
      - reply - what the supplier replied
    '''
    thread_cols = ['cid','msg_id','reply_msg_id','msg_txt','reply_msg_txt','entity','reply']
    supplier_df = pd.DataFrame(columns = columns)
    thread_df = pd.DataFrame(columns = thread_cols)
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
    var = 100
    nn = ''
    for supp in suppliers:
        for index,msg in enumerate(supp.messages[::-1]):
            if index == len(supp.messages)-1:
                #if its last message do this
                cid = msg.conversationId
                id_ = msg.id
                assi_id = None
                timestamp = None
                is_bot = msg.is_bot
                message = msg.eventDescription
                is_reply = False
                thread_id = None
            elif index == 0:
                #for the begining of conversation
                cid = msg.conversationId
                id_ = msg.id
                assi_id = msg.assignedId
                timestamp = msg.timestamp
                is_bot = msg.is_bot
                is_reply = False
                thread_id = None
                message = msg.text
            else:
                # for normal messages
                cid = msg.conversationId
                id_ = msg.id
                assi_id = msg.assignedId
                timestamp = msg.timestamp
                is_bot = msg.is_bot
                is_reply = False
                thread_id = None
                message = msg.text
                # get the threads here
                if is_bot:
                    msg_id = msg.id
                    msg_txt = msg.text
                    nn = msg_txt
                    #temp = msg.text
                    entity = msg.text
                    tid = id_
                    var = 1
                else:
                    reply_msg_id = msg.id
                    reply_msg_txt = msg.text
                    tt = nn.split('\n')
                    is_reply = True
                    thread_id = tid
                    temp = []
                    for i in tt:
                        if reply_msg_txt in i:
                            temp.append(i)
                    reply = temp[-1][2:]
                    var -= 1
            if var == 0:
                # append the row to the dataframe
                thread_df = thread_df.append({'cid':cid,'msg_id':msg_id,'reply_msg_id': reply_msg_id,'msg_txt':msg_txt,'reply_msg_txt':reply_msg_txt,'entity':entity,'reply':reply},ignore_index = True)

            #append the row to the dataframe
            supplier_df = supplier_df.append({'cid':cid,'id':id_,'assi_id': assi_id,'timestamp':timestamp,'is_bot':is_bot,'message':message,'is_reply':is_reply,'thread_id':thread_id},ignore_index  = True)

    print(supplier_df)
    #save it to a csv file
    print(thread_df)
    supplier_df.to_csv('sample.csv')
    thread_df.to_csv('thread.csv')
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='lets find some suppliers')
    parser.add_argument('--csv_path', type=str,
                      help='csv containing supplier data')
    args = parser.parse_args()
    print(args.csv_path)
    process(args.csv_path)
