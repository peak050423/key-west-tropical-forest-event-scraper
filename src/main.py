import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime
import requests
import time
from apify import Actor


url = "https://www.keywest.garden/garden-calendar/"

headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-US,en;q=0.9,fr;q=0.8",
    "cache-control": "max-age=0",
    "referer": "https://www.google.com/",
    "sec-ch-ua": '"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "Windows",
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "cross-site",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"
}

async def main():
    async with Actor:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:

            ld_json_data = []
            soup = BeautifulSoup(response.text, "html.parser")

            ld_json_scripts = soup.find_all("script", type="application/ld+json")
            if ld_json_scripts:
                for script in ld_json_scripts:
                    ld_json_data.append(json.loads(script.string))

            records = []
            unique_identifiers = set() 
            
            for record in ld_json_data:
                if not "@graph" in ld_json_data:

                    date = record.get("startDate")
                    name = record.get("name")
                    description = record.get("description")

                    identifier = (date, name, description)

                    date_obj = datetime.strptime(date, '%Y-%m-%d').date() if date else None

                    if not record.get('location').get('name') and not record.get('location').get('address'):
                        location = f"{record.get('location').get('name')}, {record.get('location').get('address')}"
                    else: 
                        location = ""

                    if date and name:
                        if identifier not in unique_identifiers:            
                            records.append({
                                "date": date,
                                "name": name,
                                "location": location,
                                "description": description,
                                "image": record.get("image"),
                                "url": record.get("url")
                            })


            records.sort(key=lambda x: x['date'])

            results = []

            for record in records:

                response = requests.get(record.get("url"), headers=headers)

                if response.status_code == 200:
                    
                    soup = BeautifulSoup(response.text, "html.parser")
                    cost = ""
                    time = ""
                    start_time = ""
                    end_time = ""

                    def contains_am_pm(tag):
                        return tag.name == "abbr" and ("am" in tag.text or "pm" in tag.text)
                    
                    if soup.find(contains_am_pm):
                        time = soup.find(contains_am_pm).get_text(strip=True).split(" – ")  

                    if len(time) > 0:
                        start_time = time[0]
                        end_time = time[1]
                        start_time_object = datetime.strptime(start_time.strip(), '%I:%M %p')
                        start_time_24 = start_time_object.strftime('%H:%M')
                        end_time_object = datetime.strptime(end_time.strip(), '%I:%M %p')
                        end_time_24 = end_time_object.strftime('%H:%M')
                        start_time = start_time_24
                        end_time = end_time_24
                    if soup.find("dd", class_="mec-events-event-cost"):
                        cost = soup.find("dd", class_="mec-events-event-cost").get_text(strip=True)

                results.append(
                    {
                        "date": record.get('date'),
                        "start_time": start_time,
                        "end_time": end_time,
                        "name": record.get('name'),
                        "cost": cost,
                        "location": record.get('location'),
                        "image": record.get('image'),
                        "description": record.get('description'),
                    }
                )

            results.sort(key=lambda x: x['date'])
            
            for row in results:
                await Actor.push_data(
                    {
                        "date": row.get('date'),
                        "start_time": row.get('start_time'),
                        "end_time": row.get("end_time"),
                        "name": row.get('name'),
                        "cost": row.get('cost'),
                        "location": row.get('location'),
                        "image": row.get('image'),
                        "description": row.get('description')
                    }
                )

        else:
            print("Failed to retrieve the page. Status code:", response.status_code)
