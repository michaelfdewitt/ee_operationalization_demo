import functions_framework
from google.cloud import bigquery
import ee
import time
import google.auth

# Note that this is a gen 2 Cloud Function!
# Pre-config: https://code.earthengine.google.com/9ae153b16eed01af49239840ec02e741

@functions_framework.cloud_event
def write_to_bq(request):
     # Set up auth.
    credentials, project_id = google.auth.default()
    ee.Initialize(credentials, project="mdewitt-joonix")
    table_id = "mdewitt-joonix.ee_export.fire_stats"
    
    ## Get BiqQuery configured
    client = bigquery.Client()
    table = client.get_table(table_id)

    # Fetch the current time.
    epoch = int(time.time_ns() / 1_000_000)
    now = ee.Date(epoch)

    # Find our image.
    goes = ee.ImageCollection("NOAA/GOES/16/FDCC")
    geometry = ee.Geometry.Point([-124.06675059757329, 42.554040120698026])
    region = geometry.buffer(10 * 1000, 100);
    image = goes.select(['Area', 'Temp']).filterDate(now.advance(-10, 'minutes'), now).first()

    # Return the fire stats for the given image.
    dict = image.reduceRegion(ee.Reducer.sum(), region, 1000, 'EPSG:4326')
    d = ee.Date(image.get('system:time_start'))
    dict = dict.set('date', d.format("YYYY-MM-dd HH:mm:ss.SSS")).getInfo()
    print(dict)

    errors = client.insert_rows_json(table, [dict])  # BQ insert.

    if errors == []:
        print("Data Loaded")
        return "Success"
    else:
        print(errors)
        return "Failed"
