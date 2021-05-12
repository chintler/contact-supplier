import os

#export WATI_SERVER_URL=live-server-940.wati.io
#export WATI_SERVER_RETRIEVE_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiJiZjZiOTNlMC00ZjNlLTQ4ZGYtYWQ2My04M2U3N2EwMmQ1YTMiLCJ1bmlxdWVfbmFtZSI6InNoYW50YW51QGVrYXRyYS5vbmUiLCJuYW1laWQiOiJzaGFudGFudUBla2F0cmEub25lIiwiZW1haWwiOiJzaGFudGFudUBla2F0cmEub25lIiwiaHR0cDovL3NjaGVtYXMubWljcm9zb2Z0LmNvbS93cy8yMDA4LzA2L2lkZW50aXR5L2NsYWltcy9yb2xlIjoiQURNSU5JU1RSQVRPUiIsImV4cCI6MjUzNDAyMzAwODAwLCJpc3MiOiJDbGFyZV9BSSIsImF1ZCI6IkNsYXJlX0FJIn0.ncsrllceUk8E68o3JJfL-oAPBre1btSjAaIj426JL2I
#export WATI_SERVER_SEND_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiJiNGYzMjRiOS1mZTcwLTRmMzItOWMxZS1kZTM2Njg5Mzk5ODIiLCJ1bmlxdWVfbmFtZSI6ImppdGVzaEBjbGFyZS5haSIsIm5hbWVpZCI6ImppdGVzaEBjbGFyZS5haSIsImVtYWlsIjoiaml0ZXNoQGNsYXJlLmFpIiwiaHR0cDovL3NjaGVtYXMubWljcm9zb2Z0LmNvbS93cy8yMDA4LzA2L2lkZW50aXR5L2NsYWltcy9yb2xlIjoiRVhURVJOQUxfQURNSU5JU1RSQVRPUiIsImV4cCI6MjUzNDAyMzAwODAwLCJpc3MiOiJDbGFyZV9BSSIsImF1ZCI6IkNsYXJlX0FJIn0.NgKDMPIHKDnSPUu6Q9j0zecVslQYYtkxw42qtmEVU_E
WATI_SERVER_URL = os.environ.get('WATI_SERVER_URL')
WATI_SERVER_RETRIEVE_TOKEN = os.environ.get('WATI_SERVER_RETRIEVE_TOKEN')
WATI_SERVER_SEND_TOKEN = os.environ.get('WATI_SERVER_SEND_TOKEN')
MIN_POOL_SIZE = 300

get_message_url = "https://{0}/api/v1/getMessages/{1}"
get_message_payload = {'pageSize': '100', 'pageNumber': '1'}
get_message_files = []
get_message_headers = {'Authorization': 'Bearer {}'.format(WATI_SERVER_RETRIEVE_TOKEN),  'Cookie': '__cfduid=dc533c9ba4cb8c277f79928e3978a52251619958624'}



send_template_url = "https://{0}/api/v1/sendTemplateMessage/{1}"

send_template_payload="{\n  \"template_name\": \"{0}\",\n  \"broadcast_name\": \"{1}\",\n  \"parameters\": \"[{'name':'name', 'value':'John'}, {'name':'ordernumber', 'value':'12345'}]\"\n}"
send_template_headers = {
  'Authorization': 'Bearer {}'.format(WATI_SERVER_SEND_TOKEN),
  'Content-Type': 'application/json',
  'Cookie': '__cfduid=dc533c9ba4cb8c277f79928e3978a52251619958624'
}
