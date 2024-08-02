
import json
from urllib.request import urlopen 
import requests


def main():

    url = "http://sem-prod-google-ads-api.service.gcp-us-west-1.consul/api/query?query=SELECT click_view.gclid,campaign.id,click_view.area_of_interest.country, click_view.location_of_presence.country FROM click_view WHERE segments.date = '2024-07-10'&parent_account_id=3418667033&account_id=1677426154"
    r = requests.get(url)
    data = r.json()
    #print(data['data'][0]["clickView"]["gclid"])
    # data = json.dumps(data)
    res = {"India": [],'NOT - India' :[] }
    for item in data["data"]:
        if item["clickView"]["locationOfPresence"]["country"] ==  "geoTargetConstants/2356":
           res['India'].append(item["clickView"]["gclid"])
           print('Gclid for India = '+ item["clickView"]["gclid"])
        else:
            res['NOT - India'].append(item["clickView"]["gclid"])
            print("Gclid Other than India ="+ item["clickView"]["gclid"])
        # res.append(item["clickView"]["gclid"])
    with open("Gclid.json", "w") as file:
        json.dump(res, file)
    #print(len(res))


if __name__ == "__main__":
    main()

