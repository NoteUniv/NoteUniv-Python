import requests, json, re

# Intructor endpoint
# https://monemploidutemps.unistra.fr/api/resource/instructor.json/
# Student endpoint
# https://monemploidutemps.unistra.fr/api/resource/trainee.json/

# Old GUI
# https://adewebcons.unistra.fr/jsp/standard/gui/interface.jsp

global_url = "https://monemploidutemps.unistra.fr/api/resource/7863.json/"
# Change the project id for different year (10 for 2019-2020 / 12 for 2020-2021)
base_url = "https://adewebcons.unistra.fr/jsp/custom/modules/plannings/anonymous_cal.jsp?resources={},{}&projectId=12&calType=ical&nbWeeks=100"
json_data_final = {}
temp_class = None

data_global = requests.get(global_url).json()

for x in data_global["children"]:
    if not x["name"].startswith("_"):
        i = 0
        url_section = x["id"]
        name_section = x["name"]
        json_data_final[name_section] = {}
        data_section = requests.get(url_section).json()
        for y in data_section["children"]:
            if not temp_class:
                temp_class = int(re.findall(r"\d+", y["id"])[0])
            else:
                id_class = int(re.findall(r"\d+", y["id"])[0])
                i += 1
                json_data_final[name_section].update({"TP" + str(i): base_url.format(temp_class, id_class)})
                temp_class = None

with open("edt_url.json", "w", encoding="utf-8") as file:
    json.dump(json_data_final, file, indent=4)
