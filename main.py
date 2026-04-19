import requests
from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional
from concurrent.futures import ThreadPoolExecutor
import json
import re
import os

api = os.getenv('API_KEY', '')
base_url = os.getenv('BASE_URL', 'https://trakerxo.xyz/admin_api/v1')
api_headers = {
 "Api-Key": api
}

campaign_filters = []
report_payload = {
 "measures": [
 "profitability",
 "clicks",
 "campaign_unique_clicks",
 "conversions",
 "roi_confirmed"
 ],
 "filters": [],
 "sort": [
 {
 "name": "clicks",
 "order": "desc"
 }
 ],
 "range": {
 "interval": "today",
 "timezone": "Asia/Kolkata"
 },
 "dimensions": [
 "campaign_id"
 ],
 "offset": 0
}
active_camps = []
all_streams = []
filtered_streams = []
updated_streams = []

class RequestData(BaseModel):
 filters_include: list[str] = []
 filters_exclude: list[str] = []
 old_landing: Optional[int] = None
 new_landing: Optional[int] = None

class RequestDataAdd(BaseModel):
 filters_include: list[str] = []
 filters_exclude: list[str] = []
 add_camps: Optional[str] = None
 specific_camps: Optional[list[int]] = None
 remove_previous: Optional[str] = None
 add_landings: Optional[list[int]] = None

def report_filters(inc,exc):
 report_payload["filters"].clear()
 if 'na' not in inc:
 for i in inc:
 if re.fullmatch(r"\d+", i):
 campaign_filters.append({
 "name": "campaign",
 "operator": "MATCH_REGEXP",
 "expression": f"(^|[^0-9]){i}([^0-9]|$)"
 })
 else:
 campaign_filters.append({
 "name": "campaign",
 "operator": "CONTAINS",
 "expression": f"{i}"
 })
 if 'na' not in exc:
  for i in exc:
   if re.fullmatch(r"\d+", i):
    campaign_filters.append({
     "name": "campaign",
     "operator": "NOT_MATCH_REGEXP",
     "expression": f"(^|[^0-9]){i}([^0-9]|$)"
    })
   else:
    campaign_filters.append({
     "name": "campaign",
     "operator": "NOT_CONTAIN",
     "expression": f"{i}"
    })
 for i in campaign_filters:
  report_payload["filters"].append(i)



def report_build():
 report_url = f'{base_url}/report/build'
 response = requests.post(report_url, json=report_payload, headers=api_headers)
 full_report = response.json()
 print(f'full report: {full_report}')
 print("Report Fetched - Successfully👍")
 print(json.dumps(report_payload))
 camps_extraction(full_report)

def camps_extraction(report):
 for data in report['rows']:
 active_camps.append(data['campaign_id'])
 print(f"Active Camps:{len(active_camps)}")

def fetch_streams(camp):
 report_url = f'{base_url}/campaigns/{camp}/streams'
 response = requests.get(report_url, headers=api_headers)
 all_streams.extend(response.json())

def filter_streams(streams, act):
 for stream in streams:
 if stream["state"] == "active" and stream['type'] == "regular" and len(stream['landings']) > 0:
 try:
 if (act == 'replace' or act == 'remove'):
 for landing in stream['landings']:
 landing_id = landing['landing_id']
 if landing_id == old_landing:
 filtered_streams.append(stream)
 elif act == 'add':
 campaign_id = stream['campaign_id']
 print(campaign_id)
 if add_camps == 'Specific':
 for id in specific_camps:
 if campaign_id == int(id):
 filtered_streams.append(stream)
 elif add_camps == 'all':
 filtered_streams.append(stream)
 except:
 print(f"Stream: {stream['id']}, Campaign: {stream['campaign_id']}")

def weight_update(stream):
 for s in stream:
 landings = s['landings']
 n = len(landings)
 if n == 0:
 continue
 base = 100 // n
 remainder = 100 % n
 for i, landing in enumerate(landings):
 if i < remainder:
 landing["share"] = base + 1
 else:
 landing["share"] = base
 stream_update(updated_streams)

def stream_change(filter, act):
 print('in stream_change')
 print(act)
 for stream in filter:
 already_exist = False
 if 'created_at' in stream:
 stream.pop('created_at')
 if len(stream['filters']) > 0:
 if 'id' in stream['filters'][0]:
 stream['filters'][0].pop('id')
 if 'stream_id' in stream['filters'][0]:
 stream['filters'][0].pop('stream_id')
 if 'oid' in stream['filters'][0]:
 stream['filters'][0].pop('oid')
 for landing in stream['landings']:
 if landing['landing_id'] == new_landing:
 already_exist = True
 for landing in stream['landings']:
 if "id" in landing:
 landing.pop('id')
 if "created_at" in landing:
 landing.pop('created_at')
 if "updated_at" in landing:
 landing.pop('updated_at')
 if landing['landing_id'] == old_landing:
 if act == 'replace' and already_exist == False:
 print('in replace')
 landing['landing_id'] = new_landing
 elif act == 'remove' or (act == 'replace' and already_exist == True):
 print('in remove')
 if len(stream['landings']) > 1:
 stream['landings'].remove(landing)
 print('landing removed')
 else:
 landing['landing_id'] = new_landing
 updated_streams.append(stream)
 weight_update(updated_streams)

def add_new_landing(stream, landings):
 for st in stream['landings']:
 for landing in landings:
 if st['landing_id'] == landing:
 index = landings.index(landing)
 landings.pop(index)
 for landing in landings:
 str = {
 'stream_id': stream['id'], 'landing_id': landing, 'state': 'active', 'share': 50,
 }
 stream['landings'].append(str)
 updated_streams.append(stream)

def stream_change_add(camps, prev, land):
 type_data = filtered_streams
 if prev == "yes":
 for i in type_data:
 landings_len = len(i['landings'])
 for position in range(landings_len):
 i["landings"].pop(0)
 add_new_landing(i, land)
 weight_update(updated_streams)
 elif prev == "no":
 for i in type_data:
 add_new_landing(i, land)
 print(f'nfs{json.dumps(updated_streams)}"')
 weight_update(updated_streams)

def stream_update(final_stream):
 for stream in final_stream:
 updated_payload = json.dumps(stream)
 print(updated_payload)
 response = requests.put(f'{base_url}/streams/{stream["id"]}', data=updated_payload, headers=api_headers)
 print(response)

app = FastAPI()

@app.post("/remove")
def run(data: RequestData):
 campaign_filters.clear()
 active_camps.clear()
 all_streams.clear()
 filtered_streams.clear()
 updated_streams.clear()
 global filters_include, filters_exclude
 global old_landing, new_landing
 filters_include = data.filters_include
 filters_exclude = data.filters_exclude
 action = 'remove'
 old_landing = data.old_landing
 new_landing = data.new_landing
 report_filters(filters_include, filters_exclude)
 report_build()
 with ThreadPoolExecutor(max_workers=10) as executor:
 list(executor.map(fetch_streams, active_camps))
 filter_streams(all_streams, action)
 stream_change(filtered_streams, action)
 return {
 "status": "completed",
 "campaigns": active_camps,
 "updated_streams": len(updated_streams)
 }

@app.post("/replace")
def run_replace(data: RequestData):
 campaign_filters.clear()
 active_camps.clear()
 all_streams.clear()
 filtered_streams.clear()
 updated_streams.clear()
 global filters_include, filters_exclude
 global old_landing, new_landing
 filters_include = data.filters_include
 filters_exclude = data.filters_exclude
 action = 'replace'
 old_landing = data.old_landing
 new_landing = data.new_landing
 report_filters(filters_include, filters_exclude)
 report_build()
 with ThreadPoolExecutor(max_workers=10) as executor:
 list(executor.map(fetch_streams, active_camps))
 filter_streams(all_streams, action)
 stream_change(filtered_streams, action)
 return {
 "status": "completed",
 "campaigns": active_camps,
 "updated_streams": len(updated_streams)
 }

@app.post("/add")
def run_add(data: RequestDataAdd):
 campaign_filters.clear()
 active_camps.clear()
 all_streams.clear()
 filtered_streams.clear()
 updated_streams.clear()
 global filters_include, filters_exclude
 global add_camps, specific_camps, remove_previous, add_landings
 filters_include = data.filters_include
 filters_exclude = data.filters_exclude
 action = 'add'
 add_camps = data.add_camps
 specific_camps = data.specific_camps or []
 remove_previous = data.remove_previous
 add_landings = data.add_landings or []
 report_filters(filters_include, filters_exclude)
 report_build()
 with ThreadPoolExecutor(max_workers=10) as executor:
 list(executor.map(fetch_streams, active_camps))
 filter_streams(all_streams, action)
 stream_change_add(add_camps, remove_previous, add_landings)
 return {
 "status": "completed",
 "campaigns": active_camps,
 "updated_streams": len(updated_streams)
 }
