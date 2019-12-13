import os, requests, dotenv

dotenv.load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

debug = False

token = os.environ.get("TOKEN_SEAFILE")

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
