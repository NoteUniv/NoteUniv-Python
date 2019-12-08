import os, requests, zipfile, io, dotenv, statistics, mysql.connector

dotenv.load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

debug = False

token = os.environ.get("TOKEN_SEAFILE")
host = os.environ.get("BDD_HOST")
login = os.environ.get("BDD_LOGIN")
passwd = os.environ.get("BDD_PASSWD")

r = requests.get("https://seafile.unistra.fr/api/v2.1/share-link-zip-task/?share_link_token=" + token + "&path=%2F&_=1570695690269")
if r.ok:
    token = r.json()["zip_token"]

r = requests.get("https://seafile.unistra.fr/seafhttp/zip/" + token, stream=True)

a = open("notes1.zip", "wb")
a.write(r.content)
a.close()


with open("notes.zip", "wb") as file:
    for chunk in r:
        file.write(chunk)


"""
with zipfile.ZipFile("notes.zip", "r") as zip_ref:
    for zipfile in zip_ref.infolist():
        if zipfile.filename[-1] == '/':
            continue
        zipfile.filename = os.path.basename(zipfile.filename)
        zip_ref.extract(zipfile, pdf_folder)
"""
