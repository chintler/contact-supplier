import requests
import json
import pandas as pd
import datetime
import traceback
import argparse
import utils
from classes import *

def process(csv_file_path):
    row_limiter = 99 #enter no. of suppliers  u want
    df = utils.load_suppliers_csv(csv_file_path)
    '''

    The columns are 
     - conversation_id(same for the entire thread)
     - id - message_id
     - assi_id - assigned_id
     - is_bot -> Bot if it is bot else Supplier
     - message -> text
    '''
    columns = ['cid','id','assi_id','mobile','timestamp','is_bot','message','is_reply','thread_id']
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
    thread_cols = ['cid','mobile','msg_id','reply_msg_id','msg_txt','reply_msg_txt','entity','reply','comments']
    supplier_df = pd.DataFrame(columns = columns)
    thread_df = pd.DataFrame(columns = thread_cols)
    suppliers = []

    for rownum, row_data in enumerate(df.iterrows()):
        row = row_data[1]
        print(rownum, row['Name'])
        supplier = Supplier(name=row['Name'], pincode=440004, contact_num=row['Phone'], supplier_type='material')
        suppliers.append(supplier)
        if rownum == row_limiter:
            break

    utils.multiprocess_supplier_messages(suppliers)
    
    nn = ''
    for s_ind,supp in enumerate(suppliers):
        print(f'**************************supp{s_ind}********************************************')
        contact_num = supp.contact_num
        var = 100
        thread_start_flag = False
        thread_end_flag = False
        for index,msg in enumerate(supp.messages[::-1]):

            #print(f'----------------------index:{index}------------------------------------------------------------------------')
            if msg.eventDescription:
                msg_txt = msg.eventDescription
                #print(f'for index {index} eventDescription is {msg_txt}')
                if hasattr(msg,'type'):
                    if msg.type == 4:
                        print('ended')
                        cid = msg.conversationId
                        id_ = msg.id
                        assi_id = None  
                        timestamp = None
                        is_bot = msg.is_bot
                        
                        is_reply = False
                        thread_id = None
                        break
                    else:
                        cid = msg.conversationId
                        id_ = msg.id
                        assi_id = None
                        timestamp = None
                        is_bot = msg.is_bot
                        is_reply = False
                        thread_id = None
                else:
                    cid = msg.conversationId
                    id_ = msg.id
                    assi_id = None
                    timestamp = None
                    is_bot = msg.is_bot
                    is_reply = False
                    thread_id = None
            else:
                msg_txt = msg.text
                #print(f'for index {index} message is {msg_txt}')
                cid = msg.conversationId
                id_ = msg.id
                assi_id = msg.assignedId
                timestamp = msg.timestamp
                is_bot = msg.is_bot
                is_reply = False
                thread_id = None
                
                if msg_txt and msg_txt.find('English') != -1:
                    print('Thread started here -------------------------')
                    thread_start_flag = True

                if thread_start_flag:
                    
                    if is_bot:
                        #print(f' for bot came here for index{index}')
                        msg_id = msg.id
                        msg_txt = msg.text
                        nn = msg_txt
                        #temp = msg.text
                        entity = msg.text
                        tid = id_
                        var = 1
                    else:
                        #print(f'4 supp came here for index{index}')
                        reply_msg_id = msg.id
                        reply_msg_txt = msg.text
                        comments = None
                        #check if the reply is not a number and add to comments
                        reply_flag = True
                        if reply_msg_txt:
                            if not reply_msg_txt.isdigit():
                                comments = reply_msg_txt
                                reply_flag = False
                        else:
                            reply_msg_txt = None
                            reply_flag = False
                        tt = nn.split('\n')
                        is_reply = True
                        reply = None
                        thread_id = tid
                        if reply_flag:
                            temp = []
                            flag = False
                            # Check if the reply is as expected by the flow (True if yes else false)
                            for i in tt:
                                if reply_msg_txt in i:
                                    flag = True
                                    temp.append(i)
                            #if reply is as expected then update the reply field
                            if flag:
                                reply = temp[-1][2:]
                        var -= 1
                if var == 0:
                    # append the row to the dataframe
                    thread_df = thread_df.append({'cid':cid,'mobile':contact_num,'msg_id':msg_id,'reply_msg_id': reply_msg_id,'msg_txt':nn,'reply_msg_txt':reply_msg_txt,'entity':entity,'reply':reply,'comments':comments},ignore_index = True)
            #append the row to the dataframe
            supplier_df = supplier_df.append({'cid':cid,'id':id_,'assi_id': assi_id,'mobile':contact_num,'timestamp':timestamp,'is_bot':is_bot,'message':msg_txt,'is_reply':is_reply,'thread_id':thread_id},ignore_index  = True)

    
        #thread_df.to_csv(string)
    #print(supplier_df)
    #save it to a csv file
    supplier_df.to_csv('suppliers_df.csv')
    thread_df.to_csv('thread_df.csv')
if __name__ == "__main__":
    #parser = argparse.ArgumentParser(description='lets find some suppliers')
    #parser.add_argument('--csv_path', type=str,
    #                  help='csv containing supplier data')
    #args = parser.parse_args()
    #print(args.csv_path)
    process('Contacts.csv')
