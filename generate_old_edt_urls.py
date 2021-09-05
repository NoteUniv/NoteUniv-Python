import requests, json, re

# Old URL
# https://adewebcons.unistra.fr/jsp/standard/gui/interface.jsp
# New URL
# https://adecons.unistra.fr/direct/index.jsp

global_url = "https://monemploidutemps.unistra.fr/api/resource/7863.json/"
# Change the project id for different year
year_id = "8"
base_url = "https://adecons.unistra.fr/jsp/custom/modules/plannings/anonymous_cal.jsp?resources={},{}&projectId=" + year_id + "&calType=ical&nbWeeks=999"

# Token is generated on https://monemploidutemps.unistra.fr/, select Bearer
headers = {"Authorization": "Bearer " + input("Token Bearer: ")}

json_data_final = {}
temp_class = None

data_global = requests.get(global_url, headers=headers).json()

for x in data_global["children"]:
    if not x["name"].startswith("_"):
        i = 0
        url_section = x["id"]
        name_section = x["name"]
        json_data_final[name_section] = {}
        data_section = requests.get(url_section, headers=headers).json()
        for y in data_section["children"]:
            if not temp_class:
                temp_class = int(re.findall(r"\d+", y["id"])[0])
            else:
                id_class = int(re.findall(r"\d+", y["id"])[0])
                i += 1
                json_data_final[name_section].update({"TP" + str(i): base_url.format(temp_class, id_class)})
                temp_class = None
        temp_class = None

with open("edt_url.json", "w", encoding="utf-8") as file:
    json.dump(json_data_final, file, indent=4)
