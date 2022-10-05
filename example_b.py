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

"""Generates an Earth Engine Asset from a CSV in a Cloud Storage Bucket."""

import ee
import google.auth

def gcs_to_ee(event, context):
  """Creates an Earth Engine asset from data in Cloud Storage.

  When an object is created in a specified Cloud Storage Bucket, an Earth Engine
  task that creates an asset from that object.

  Args:
    event: Event payload.
  """
  # Set up auth.
  credentials, project_id = google.auth.default()
  ee.Initialize(credentials)

  # Get GCS location of file creation that triggered function.
  file = event
  print(event)
  path = "gs://" + file["bucket"] + "/" + file["name"]
  file_title = file["name"].rsplit(".", 1)[0]
  print(f"Ingesting {file_title} from {path}")

  # Trigger the ingestion.
  ingest_asset(project_id, file_title, path)

def ingest_asset(project_id, file_title, path):
  # Create request id
  request_id = ee.data.newTaskId()[0]

  # Create params for ingestion
  name = "projects/" + project_id + "/assets/" + file_title
  sources = [{"uris": [path]}]
  params = {"name": name, "sources": sources}

  # Start ingestion
  print("Starting ingestion task.")
  print(ee.data.startTableIngestion(request_id, params, allow_overwrite=True))

