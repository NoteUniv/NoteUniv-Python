from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage
import os, requests, zipfile, io, dotenv, statistics, mysql.connector

dotenv.load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

token = os.environ.get("TOKEN_SEAFILE")
host = os.environ.get("BDD_HOST")
login = os.environ.get("BDD_LOGIN")
passwd = os.environ.get("BDD_PASSWD")
pdf_folder = "notes/"
is_existing = 1
msg_nom_pren = "Saisir : NOM et Prénom  Enseignant"
msg_type_epr = "Sélectionner : type d'épreuve"
msg_type_note = "Sélectionnez  : type de note"
msg_nom_module = "Saisir : Nom du Module et Nom du Devoir"

"""
r = requests.get("https://seafile.unistra.fr/api/v2.1/share-link-zip-task/?share_link_token=" + token + "&path=%2F&_=1570695690269")
if r.ok:
    token = r.json()["zip_token"]
r = requests.get("https://seafile.unistra.fr/seafhttp/zip/" + token)
a = open("notes.zip", "wb").write(r.content)

with zipfile.ZipFile("notes.zip", "r") as zip_ref:
    for zipfile in zip_ref.infolist():
        if zipfile.filename[-1] == '/':
            continue
        zipfile.filename = os.path.basename(zipfile.filename)
        zip_ref.extract(zipfile, pdf_folder)
"""

def convert_pdf_to_txt(path):
    output = io.StringIO()
    manager = PDFResourceManager()
    converter = TextConverter(manager, output, laparams=LAParams())
    interpreter = PDFPageInterpreter(manager, converter)

    infile = open(path, 'rb')
    for page in PDFPage.get_pages(infile, {0}):
        interpreter.process_page(page)
    infile.close()
    converter.close()
    text = output.getvalue()
    output.close()
    return text.split("\n")

mydb = mysql.connector.connect(user=login, password=passwd, host=host, database="test")
mycursor = mydb.cursor()

for filename in os.listdir(pdf_folder):
    list_el = [x for x in convert_pdf_to_txt(pdf_folder + filename) if x != ""]

    type_note = list_el[list_el.index(msg_type_note) + 1]
    type_epreuve = list_el[list_el.index(msg_type_epr) + 1]
    name_devoir = list_el[list_el.index(msg_nom_module) + 1]
    name_ens = list_el[list_el.index(msg_nom_pren) + 1]
    link_pdf = "https://seafile.unistra.fr/d/" + token + "/files/?p=/" + filename + "&dl=1"
    y, m, d, _ = filename.split("_", 3)
    note_date = f"{d}/{m}/{y}"

    etu_start_index = list_el.index("N° Etudiant")
    nb_etu = int(list_el[etu_start_index - 1])
    num_etu = list_el[etu_start_index + 1:etu_start_index + nb_etu + 1]
    note_start_index = list_el.index("Note")
    note_etu = list_el[note_start_index + 1:note_start_index + nb_etu + 1]

    notes_total = len([x for x in note_etu if x != " " and x != "ABI"])
    moy = statistics.mean([float(x.replace(",", ".")) for x in note_etu if x != " " and x != "ABI"])
    median = statistics.median([float(x.replace(",", ".")) for x in note_etu if x != " " and x != "ABI"])
    mini = min([float(x.replace(",", ".")) for x in note_etu if x != " " and x != "ABI"])
    maxi = max([float(x.replace(",", ".")) for x in note_etu if x != " " and x != "ABI"])
    variance = statistics.variance([float(x.replace(",", ".")) for x in note_etu if x != " " and x != "ABI"])
    deviation = statistics.stdev([float(x.replace(",", ".")) for x in note_etu if x != " " and x != "ABI"])

    dict_etu_note = list(zip(num_etu, note_etu))
    # note_etu = [x[1] for x in dict_etu_note if x[0] == "21901316"][0]
    sql = "SELECT name_devoir,COUNT(*) as count FROM note GROUP BY name_devoir ORDER BY count DESC"
    is_existing = mycursor.execute(sql)
    print(is_existing)
    if int(is_existing) == 0:
        for key, value in dict_etu_note:
            id_etu = int(key)
            note_etu = float(value.replace(",", ".")) if value != " " else 0
            sql = "INSERT INTO note (id_etu, note_etu, name_devoir, name_ens, note_date, type_note, type_epreuve, link_pdf) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            val = (id_etu, note_etu, name_devoir, name_ens, note_date, type_note, type_epreuve, link_pdf)
            mycursor.execute(sql, val)

mydb.commit()

mydb.close()
