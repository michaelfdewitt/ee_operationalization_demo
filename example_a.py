# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
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

def handle_event(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    # Note that we don't actually care about the message here.
    credentials, project_id = google.auth.default();
    print(project_id)
    ee.Initialize(credentials, project='personal-earthengine');
    export_composite(project_id)
    
def export_composite(project_id):
    collectionName = f"projects/{project_id}/assets/weeklyGoes"

    # Fetch the current time.
    epoch = int(time.time_ns() / 1_000_000)
    now = ee.Date(epoch)
  
    # Jump back exactly one week.
    backAWeek = now.advance(-1, 'week');  
    # Get the date range for that whole week.
    dateRange = backAWeek.getRange('week');

    # This is our ROI.    
    tiger = ee.FeatureCollection("TIGER/2018/States")
    geometry = (
        tiger
        .filterMetadata('NAME', 'equals', 'California')
        .first()
        .geometry()
    )

    # Filter the images over the area during the time of interest.
    goes = (
        ee.ImageCollection("NOAA/GOES/16/FDCC")
        .filter(ee.Filter.date(dateRange.start(), dateRange.end()))
        .filterBounds(geometry)
        .select('Temp')
    )

    # Compute the max temp over the duration.
    composite = (
        goes
        .max()
        .set('system:time_start', dateRange.start())
    )

    start = dateRange.start().format('YYYY-MM-dd').getInfo()
    end = dateRange.end().format('YYYY-MM-dd').getInfo()
    name = f"{start}_to_{end}_composite"
    print(f"Exporting image {name}")

    # Our output projection and region.
    projection = goes.first().projection().getInfo();
    region = geometry.transform(goes.first().projection(), 1000)

    # Start an export task.
    task = ee.batch.Export.image.toAsset(
        image=composite,
        description='compositeExport',
        assetId=f"{collectionName}/{name}",
        region=region,
        crs=projection.get('wkt'),
        crs_transform=projection.get('transform'),
    )
    task.start()
    print(f"Started task {task.id}.")
 
