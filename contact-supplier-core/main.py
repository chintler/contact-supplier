import requests
import json
import pandas as pd
import datetime
import traceback
import argparse
import utils
from classes import *
global nlp
nlp = utils.load_model()

def get_data(df,Phone):
    global nlp
    columns = ['LANGUAGE','MOBILE','NAME','TIMESTAMP','PINCODE','MATERIAL','AVAILABILITY','DURATION']
    new_df = pd.DataFrame(columns=columns)
    d = {}
    c = 0
    timestamp = None
    for index,i in enumerate(df['msg_txt']):
        timestamp = df.loc[index,'timestamp']
        #print('i',i)
        doc = nlp(i)
        enta = [(ent.text, ent.label_) for ent in doc.ents]
        #print('enta',enta)
        if enta and enta[0][1]:
            ent = enta[0][1]
            #print('ent',ent)
        if ent == 'SESSION' or ent == 'HERO':
            continue
        if ent:
            rp = df.loc[index,'reply']
            #print('reply',rp)
            if rp:
                d[ent] = rp
                c += 1
            else:
                d[ent] = df.loc[index,'reply_msg_txt']
                c += 1
    if c !=0:
        d['MOBILE'] = Phone
    d['TIMESTAMP'] = timestamp
    new_df = new_df.append(d,ignore_index=True)
    return new_df


def process(df):
    """
    df must be a dataframe of Name, pincode, contact_num and material, with additional columns.
    """
    row_limiter = 262 # no. of suppliers to get messages for
    '''

    The columns are 
     - conversation_id(same for the entire thread)
     - id - message_id
     - assi_id - assigned_id
     - is_bot -> Bot if it is bot else Supplier
     - message -> text
    '''
    columns = ['cid','id','assi_id','phone','timestamp','is_bot','message','is_reply','thread_id']
    cols = ['LANGUAGE','MOBILE','NAME','TIMESTAMP','PINCODE','MATERIAL','AVAILABILITY','DURATION']
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
    thread_cols = ['cid','mobile','timestamp','msg_id','reply_msg_id','msg_txt','reply_msg_txt','entity','reply','comments']
    supplier_df = pd.DataFrame(columns = columns)
    final_df = pd.DataFrame(columns = cols)
    thread_df = pd.DataFrame(columns = thread_cols)
    suppliers = []

    for rownum, row_data in enumerate(df.iterrows()):
        row = row_data[1]
        supplier = Supplier(name=row['given_name'], pincode=row.get('pincode',None), contact_num=row['phone'], supplier_type=row.get('category',None), chosen_language=row.get('language',None))
        suppliers.append(supplier)
        if rownum == row_limiter:
            break

    receiving_completed = utils.multiprocess_supplier_messages(suppliers)
    
    nn = ''
    for s_ind,supp in enumerate(suppliers):
        # print('**************************supp{s_ind}********************************************')
        contact_num = supp.contact_num
        var = 100
        thread_start_flag = False
        thread_end_flag = False
        temp_df = pd.DataFrame(columns = thread_cols)
        for index,msg in enumerate(supp.messages[::-1]):

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
                    #print('Thread started here -------------------------')
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
                    temp_df = temp_df.append({'cid':cid,'mobile':contact_num,'msg_id':msg_id,'timestamp':timestamp,'reply_msg_id': reply_msg_id,'msg_txt':nn,'reply_msg_txt':reply_msg_txt,'entity':entity,'reply':reply,'comments':comments},ignore_index = True)
                    thread_df = thread_df.append({'cid':cid,'mobile':contact_num,'msg_id':msg_id,'timestamp':timestamp,'reply_msg_id': reply_msg_id,'msg_txt':nn,'reply_msg_txt':reply_msg_txt,'entity':entity,'reply':reply,'comments':comments},ignore_index = True)
            #append the row to the dataframe
            supplier_df = supplier_df.append({'cid':cid,'id':id_,'assi_id': assi_id,'phone':contact_num,'timestamp':timestamp,'is_bot':is_bot,'message':msg_txt,'is_reply':is_reply,'thread_id':thread_id},ignore_index  = True)

        final_df = final_df.append(get_data(temp_df,contact_num),ignore_index=True)


    final_df.dropna(how = 'all',inplace = True)
        #thread_df.to_csv(string)
    #save it to a csv file
    return supplier_df, thread_df, suppliers,final_df
    
def get_active_conversations():
    supp_list = SupplierList()
    replied_suppliers_df = supp_list.get_introbot_repliers()
    
    supplier_df, thread_df, suppliers,final_df = process(replied_suppliers_df)
    
    supplier_df, first_last_msg_df = arrange_supplier_df(supplier_df)
    suppliers_to_msg = []
    for supp in suppliers:
        print(supp.name, supp.contact_num, supp.chosen_language)

        # Message only those who have a conversation history
        if (first_last_msg_df['phone']==supp.contact_num).any():
            try:
                supp.last_messaged_time = first_last_msg_df[first_last_msg_df['phone'] == supp.contact_num]['max'].dt.to_pydatetime()[0]
                print(supp.last_messaged_time)
                if supp.last_messaged_time < (datetime.datetime.now() - datetime.timedelta(days=1)):
                    suppliers_to_msg.append(supp)
                elif supp.last_messaged_time < (datetime.datetime.now() - datetime.timedelta(hours=12)):
                    suppliers_to_msg.append(supp)
                else:
                    print(supp.last_messaged_time,  datetime.datetime.now() - datetime.timedelta(days=1))
                    continue
            except:
                traceback.print_exc()
                print(supp.last_messaged_time)
                continue
            
    for sendable_supplier in suppliers_to_msg:
        # print("lala",sendable_supplier.name, sendable_supplier.contact_num, sendable_supplier.chosen_language, sendable_supplier.supplier_type)
        send_message(sendable_supplier)


    thread_df.to_csv('/home/konverge/Desktop/Work/wati/thread.csv')
    supplier_df.to_csv('/home/konverge/Desktop/Work/wati/supplier.csv')
    first_last_msg_df.to_csv('/home/konverge/Desktop/Work/wati/flmd.csv')

def arrange_supplier_df(df):
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%d %H:%M:%S') # 2021-05-06 18:30:26
    first_last_msg_df = df.groupby('phone')['timestamp'].agg(['min','max']).reset_index()
    first_last_msg_df['phone'] = first_last_msg_df['phone'].apply(str)
    return df, first_last_msg_df

def send_message(supplier):
    supplier.send_template()




if __name__ == "__main__":
    #parser = argparse.ArgumentParser(description='lets find some suppliers')
    #parser.add_argument('--csv_path', type=str,
    #                  help='csv containing supplier data')
    #args = parser.parse_args()
    #print(args.csv_path)
    get_active_conversations()
    # process('Contacts.csv')
