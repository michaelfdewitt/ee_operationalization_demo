# Copyright 2022 Google LLC
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     https://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import base64
import time
import ee
import google.auth
import json

def handle_event(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    # Note that we don't actually care about the message here.
    credentials, project_id = google.auth.default();
    ee.Initialize(credentials, project='personal-earthengine');
    log_redness()

def log_redness():
  # Fetch the current time.
  epoch = int(time.time_ns() / 1_000_000)
  now = ee.Date(epoch)
  
  # Look within the last two minutes.
  goes = (
      ee.ImageCollection("NOAA/GOES/17/MCMIPC")
      .filter(ee.Filter.date(now.advance(-10, 'minute'), now))
      .select('CMI_C02')
  )
  numImages = goes.size().getInfo()

  if numImages == 0:
    print('No new images')
    return
  if numImages > 1:
    raise Exception("Too many images")

  fc = ee.FeatureCollection('projects/personal-earthengine/assets/features')
  
  reduced = goes.first().reduceRegions(fc, ee.Reducer.mean(), 250)
  reducedList = reduced.toList(10).map(lambda e : {
      "mean": ee.Feature(e).get('mean'),
  })

  dataToLog = reducedList.getInfo()
  print(json.dumps({'cloud': dataToLog[0].get('mean')}))

