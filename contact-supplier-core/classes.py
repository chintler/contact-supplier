import json
import datetime
import constants
import requests
import traceback
import utils
import pprint


class Message:
 def __init__(self, message_dict):
  print("ft", message_dict.get("finalText ", None))


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
  self.eventDescription = message_dict.get("eventDescription", None)
  self.finalText = message_dict.get("finalText ", None)
  self.template = message_dict.get("template ", None)
  self.mediaHeaderLink = message_dict.get("mediaHeaderLink ", None)
  self.is_bot = True if self.operatorName else False
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
  #message.get_user()
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
  print(self.messages)
