#############################################################################################################################
#This Script pulls Taxonomy dats based on input nets for time period now to net 2 weeks.
#
#Schdule to run everyday and update data daily.
# Pull guide data from CS edges past 4 days to 2 weeks in future aprse data.
# Archive old taxonomy data file and add new ones.
#
#
#
#
# Developers : Abdul(daw284g) & Leon(lt258b)
#############################################################################################################################

from pytz import timezone
import json
import requests
from datetime import datetime,timedelta,date
import dateutil.parser, os
import pandas as pd
import shutil


url = "https://api.cld.dtvops.net/cs/catalogservice/edge/v2/api/nodes/"

logpath = r"C:\Users\JibranKhan\ATT\DAI_Docker\OVvalidator\emvalidatorcsi\ovemvalidatorcsi\home\centos\csi\logs"
os.makedirs(logpath,exist_ok=True)

special_ratings = {
"PG13":"PG-13",
"NC17":"NC-17",
"TVG":"TV-G",
"TVPG":"TV-PG",
"TV14":"TV-14",
"TVMA":"TV-MA",
"TWY7":"TV-V7",
"TWY":"TV-Y",
}



def getchannelids():
    mapData=[]
    payload = json.dumps({
    "filter": "subType==Channel and type==CatalogNode and customAttributes.preview==false",
    "sort_by": [
        "child.customAttributes.daiChannelName"
    ],
    "fields": [
        "id",
        "customAttributes.name",
        "customAttributes.daiEnabled",
        "customAttributes.daiChannelName"
    ]
    })
    headers = {
    'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    json_list = response.json()
    for item in json_list:
        if item['customAttributes']['daiEnabled'] == True:
            id = item.get('id','id not exist')
            net = item['customAttributes'].get('daiChannelName','not available')
            mapData.append((id,net))
    return mapData

def getdata(id): # network_id: '703b7a51-6290-4d92-6826-c52f4e23519f'
    # 4 weeks ago data.
    n = datetime.now() - timedelta(weeks=4)
    timestart = str(n.isoformat()).split("+")[0]
    timeend = n + timedelta(weeks= 6)
    # So it basically fetches 1 month back data, technically 4 weeks back and 2 weeks ahead
    timeend = str(timeend.isoformat()).split("+")[0]
    timecondition = f"(customAttributes.startDateTime>{timestart}Z and customAttributes.endDateTime<{timeend}Z)"


    payload = json.dumps({
    "max_child_recursion_depth": 1,
    "ids": [ id ],
    "sort_by": [
        "child.customAttributes.startDateTime"
    ],
    "fields": [
        "id",
        "status",
        "children",
        "child.customAttributes.ratings",
        "child.customAttributes.genres",
        "child.customAttributes.programType",
        "child.customAttributes.seriesTitleSimple",
        "child.customAttributes.startDateTime",
        "child.customAttributes.endDateTime",
        "child.customAttributes.gameDate"
    ],
    "child.filter": timecondition
    })
    headers = {
    'Content-Type': 'application/json'
    }
    # print(payload)
    response = requests.request("POST", url, headers=headers, data=payload)
    data = json.loads(response.text)
    return data



def utctopst(timestr,tz):
    datetime_obj = dateutil.parser.isoparse(timestr)
    now_pst = datetime_obj.astimezone(timezone(tz))
    return str(now_pst)

def refine_ratings(ratings):
    ratings =[ x.split(":")[0] for x in ratings]
    for rating in ratings:
        if rating in list(special_ratings.keys()):
            ratings.remove(rating)
            ratings.append(special_ratings.get(rating))
    return ratings

def process_data(json_dict,tz,net):
    datalist = json_dict[0]['children']

    csv_data = {}
    entry = 0
    df = pd.DataFrame()
    for child in datalist:
        customAttributes = child['customAttributes']
        programType = customAttributes.get('programType', '')
        channel = customAttributes.get('daiChannelName', '')
        genres = customAttributes.get('genres', '')
        seriesTitleSimple = customAttributes.get('seriesTitleSimple', '')
        #Comment out UTC to PST conversion code.
        startDateTime = customAttributes.get('startDateTime', '')
        endDateTime = customAttributes.get('endDateTime', '')
        startDateTime = utctopst(startDateTime,tz)
        endDateTime = utctopst(endDateTime,tz)
        ratings = refine_ratings(customAttributes.get('ratings', ''))



        game_entry = {
            'Channel Name': net,
            'Start Time': startDateTime,
            'End Time': endDateTime,
            'video_series': seriesTitleSimple,
            'video_program': programType,
            'video_genre': ' | '.join(genres),
            'video_rating': ' | '.join(ratings)
        }
        df = pd.concat([df,pd.DataFrame.from_records([game_entry])])
    return df


def create_csv(csvdata,net):
    filename = f"{net}.csv"
    filename = os.path.join(logpath,filename)
    # fields = ['Start Time', 'End Time', 'video_series', 'video_program', 'video_genre', 'video_rating', 'Game Time']
    if os.path.exists(filename):
        oldData = pd.read_csv(filename,index_col=False)
        csvdata = pd.concat([oldData,csvdata])
        csvdata.drop_duplicates(ignore_index=True,inplace=True)
    dulicates = csvdata[csvdata['Start Time'].duplicated()]
    if dulicates.empty:
        print(f"duplicates exist in {filename} file")
    csvdata = filter_records(csvdata)
    csvdata.to_csv(filename,index=False,sep= ",")

    print(f'{filename} created..!')

def filter_records(input_df):
    current_date = datetime.now(timezone('US/Pacific'))
    input_df['Start Time'] = pd.to_datetime(input_df['Start Time'],utc=True)
    input_df['End Time'] = pd.to_datetime(input_df['End Time'],utc=True)
    filtered_df = input_df[input_df['Start Time'] >= (current_date - pd.Timedelta(days=90))]
    text_columns = ['Channel Name', 'video_series', 'video_program', 'video_genre', 'video_rating']


    filtered_df[text_columns] = filtered_df[text_columns].apply(lambda x: x.str.replace(',', ' '))

    # Convert time to PST (Pacific Standard Time)
    pst = timezone('US/Pacific')
    filtered_df['Start Time'] = pd.to_datetime(filtered_df['Start Time']).dt.tz_convert(pst)
    filtered_df['End Time'] = pd.to_datetime(filtered_df['End Time']).dt.tz_convert(pst)

    return filtered_df

def gettz(tz):
    timezones = {
        'PST': 'America/Los_Angeles',
        'EST': 'America/New_York',
        'CST': 'America/Chicago',
        'MST': 'America/Phoenix'
    }
    if tz in timezones.keys():
        result = timezones.get(tz)
        return result
    gettz()

def main():
    print("JIBRAN KHAN")
    source_folder = logpath
    #copying the contents from the taxonomy channels to taxonomchannelsarchive
    if os.path.exists(source_folder):
        archive_folder = f"{source_folder}archive"
        today = date.today().strftime("%Y-%m-%d")
        archive_subfolder = os.path.join(archive_folder, f"taxonomydata_{today}")
        os.makedirs(archive_subfolder,exist_ok=True)
        file_list = os.listdir(source_folder)
        for filename in file_list:
            source_path = os.path.join(source_folder, filename)
            if os.path.isfile(source_path):
                shutil.copy2(source_path, archive_subfolder)
    else:
        print("The taxonomydata folder does not exist.")
    # We get a map of id and channelName (network ID and network)
    # [('a39b5a6b-fcba-4d31-53be-12190003e171', 'acc'), ('703b7a51-6290-4d92-6826-c52f4e23519f', 'aetv'), ('91f974cf-5346-2c4a-47c6-ad3326597a6b', 'amc'), ('61295f1d-b338-1427-befc-e0ec881a9599', 'american_heroes')]
    mapData = getchannelids()
    # We sort them based on channel names using a lambda function in python.
    mapData = sorted(mapData, key=lambda x: x[1].lower())
    tz = gettz("PST")
    for net_id,net in mapData:
        csvdata = process_data(getdata(net_id),tz,net)
        create_csv(csvdata,net)


if __name__ == "__main__":
    main()
