import json
import datetime
import constants
import requests
import traceback
import utils
import pprint

import pandas as pd
class Message:
 def __init__(self, message_dict):

  self.replySourceMessage = message_dict.get("replySourceMessage", None)
  self.text = message_dict.get("text", None)
  self.message_type = message_dict.get("type", None)
  self.data = message_dict.get("data", None)
  self.timestamp = message_dict.get("timestamp", None)
  self.owner = message_dict.get("owner", None)
  self.statusString = message_dict.get("statusString", None)
  self.avatarUrl = message_dict.get("avatarUrl", None)
  self.assignedId = message_dict.get("assignedId", None)
  self.operatorName = message_dict.get("operatorName", None)
  self.localMessageId = message_dict.get("localMessageId", None)
  self.failedDetail = message_dict.get("failedDetail", None)
  self.contacts = message_dict.get("contacts", None)
  self.id = message_dict.get("id", None)
  self.created = message_dict.get("created", None)
  self.conversationId = message_dict.get("conversationId", None)
  self.ticketId = message_dict.get("ticketId", None)
  self.event_type = message_dict.get("eventType", None)
  self.eventDescription = message_dict.get("eventDescription ", None)
  self.finalText = message_dict.get("finalText ", None)
  self.template = message_dict.get("template ", None)
  self.mediaHeaderLink = message_dict.get("mediaHeaderLink ", None)
  self.is_bot = self.operatorName if self.operatorName else 'Supplier'
  self.format()

 def format(self):
  if self.text:
      self.text = self.text.replace("\n", "").strip()
  else:
   if self.finalText:
    self.finalText = self.finalText.replace("\n", "").strip()

  if self.timestamp:
   self.timestamp = datetime.datetime.fromtimestamp(
    float(self.timestamp))


class MessageHandler:
 messages = []
 message_dict = {}

 def __init__(self):
  pass

 def get_messages(self, contact_num):
  retrieving_url = constants.get_message_url.format(constants.WATI_SERVER_URL,contact_num)
  print(retrieving_url)
  response = requests.request("GET", retrieving_url, headers=constants.get_message_headers,
         data=constants.get_message_payload,
         files=constants.get_message_files)
  self.message_dict = json.loads(response.text)
  pprint.pprint(self.message_dict)

  return self.message_dict

 def build_message(self, message_dict):
  message = Message(message_dict)
  return message

 def parse_messages(self, message_json={}):
  messages = []
  if not len(message_json.keys()):
   message_json = self.message_dict

  assert 'result' in message_json.keys()
  assert 'messages' in message_json.keys()
  assert 'items' in message_json['messages'].keys()

  if message_json['result'] != 'success':
   return messages

  for elem in message_json['messages']['items']:
   try:
    message = self.build_message(elem)
    messages.append(message)
   except:
    traceback.print_exc()
    continue
  print(len(messages), "messages found")

  # TODO: Sort messages by timestamp
  self.messages = messages
  return messages

 def get_parsed_messages(self, contact_num):
  self.get_messages(contact_num)
  return self.parse_messages()

 def send_template(self, contact_num, template_name, broadcast_name=""):

  broadcast_name =  template_name + "_" + contact_num if not len(broadcast_name) else broadcast_name
  template_send_url = constants.send_template_payload.format(constants.WATI_SERVER_URL, contact_num)

  response = requests.request("POST", template_send_url, headers=constants.send_template_headers,
         data=constants.send_template_payload.format(template_name, broadcast_name))
  self.send_template_resp = json.loads(response.text)
  #pprint.pprint(self.send_template_resp)

  return self.send_template_resp


class Supplier:
 messages = []
 message_json = {}
 last_reply = ""
 last_reply_type = ""
 is_valid = True
 msg_handler = None
 name = "BLANKNAME"
 pincode = "000000"
 contact_num = "-0"
 supplier_type = "BLANKSUPP"

 def __init__(self, name, pincode, contact_num, supplier_type):
  self.name = name
  self.pincode = pincode
  self.contact_num = contact_num
  self.supplier_type = supplier_type

 def get_messages(self):
  self.msg_handler = MessageHandler()
  self.message_json = self.msg_handler.get_messages(self.contact_num)

 def get_parsed_messages(self):
  msg_handler = MessageHandler()
  try:
   self.messages = msg_handler.get_parsed_messages(self.contact_num)
  except AssertionError:
   self.is_valid = False

 def get_last_reply_and_type(self):
  assert self.messages



class Contact:
  introbot_json_data = {}
  def __init__(self, contact_json={}):
    self.construct_from_json(contact_json)

  def construct_from_json(self, contact_json):
    self.allowBroadcast = contact_json.get('allowBroadcast',"")
    self.allowSMS  = contact_json.get('allowSMS',"")
    self.contactStatus =  contact_json.get('contactStatus',"")
    self.created =  contact_json.get('created',"")
    self.currentFlowNodeId =  contact_json.get('currentFlowNodeId',"")
    self.customParams =  contact_json.get('customParams',"")
    self.firstName =  contact_json.get('firstName',"")
    self.fullName =  contact_json.get('fullName',"")
    self.id =  contact_json.get('id',"")
    self.isDeleted =  contact_json.get('isDeleted',"")
    self.isInFlow =  contact_json.get('isInFlow',"")
    self.lastFlowId =  contact_json.get('lastFlowId',"")
    self.lastUpdated =  contact_json.get('lastUpdated',"")
    self.optedIn =  contact_json.get('optedIn',"")
    self.phone =  contact_json.get('phone',"")
    self.photo =  contact_json.get('photo',"")
    self.source =  contact_json.get('source',"")
    self.tags =  contact_json.get('tags',"")
    self.teamIds =  contact_json.get('teamIds',"")
    self.wAid =  contact_json.get('wAid',"")
    self.pincode = ""
    self.category = ""


  def get_introbot_contact(self):
    url = constants.get_introbot_contacts.format(self.phone)
    response = requests.request("GET", url, headers={}, data={})
    j = json.loads(response.text)
    self.introbot_json_data = j
    if len(self.introbot_json_data['data']) > 0:
      self.category = self.introbot_json_data['data'][0]['category']
    else:
      self.category = ""
    return j
  
  def send_patch(self, payload = {}):
    url = constants.make_patch_url
    payload='{"sb_twilio":"i"}'
    response = requests.request("PATCH", url, headers=constants.make_patch_url.format(self.phone), data=payload)
    print(response.text)

    
class SupplierList:
  supplier_json = {}
  page_nums = []
  list_of_suppliers = []
  next_page_num = 1

  def __init__(self):
    pass

  def get_suppliers(self, num_suppliers=10, page_num=1):

    # WATI contacts list
    response = requests.request("GET", 
    constants.get_contacts_url, 
    headers=constants.get_contact_headers, 
    data={'pageSize': '{0}'.format(num_suppliers),'pageNumber': '{0}'.format(page_num)},
    files=constants.get_contacts_files)
    j = json.loads(response.text)

    return j

  def parse_suppliers(self, response_json):
    # Update page_num
    self.page_nums.append(response_json['link']['pageNumber'])
    contacts = []
    if not len(response_json['contact_list']):
      raise EOFError
    for contact in response_json['contact_list']:
      # Construct with data from wati
      parsed_contact = Contact(contact)
      # Then, load up data from introbot
      parsed_contact.get_introbot_contact()
      msg_handler = MessageHandler(parsed_contact.phone)
      contacts.append(parsed_contact)
    
    self.list_of_suppliers.extend(contacts)

  
  def get_and_parse(self, num_suppliers=10, page_num=1):
    while page_num < constants.supplier_list_page_num_max:
      print("getting data for ", page_num)
      supplier_json = self.get_suppliers(num_suppliers=num_suppliers, page_num=page_num)
      try:
        self.parse_suppliers(supplier_json)
      except EOFError:
        break
      page_num+=1
  
  def turn_into_dataframe(self):
    assert len(self.list_of_suppliers)>0
    df = pd.DataFrame()
    for supp in self.list_of_suppliers:
      print(pprint.pprint(vars(supp)))
      df.append(vars(supp))
      # df.append({'name':supp.name,'pincode':supp.pincode, 'contact'})
      # (name=row['name'], pincode=row['pincode'],contact_num=row['contact'], supplier_type=row['type'])
    df.to_csv("wati_supp_df")
    pass



if __name__ == "__main__":
    supp_list = SupplierList()
    supp_list.get_and_parse()
    supp_list.turn_into_dataframe()