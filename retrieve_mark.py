from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage
from zipfile import ZipFile
import os, json, requests, zipfile, io, dotenv, statistics, mysql.connector

dotenv.load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

debug = False

token = os.environ.get("TOKEN_SEAFILE")
host = os.environ.get("BDD_HOST")
login = os.environ.get("BDD_LOGIN")
passwd = os.environ.get("BDD_PASSWD")
pdf_folder = "notes/"
msg_nom_pren = "Saisir : NOM et Prénom  Enseignant"
msg_type_epr = "Sélectionner : type d'épreuve"
msg_type_note = "Sélectionnez  : type de note"
msg_nom_module = "Saisir : Nom du Module et Nom du Devoir"

with open("subjects_coeff.json", "r", encoding="utf-8") as file:
    subjects = json.load(file)

def main():
    global token
    r = requests.get("https://seafile.unistra.fr/api/v2.1/share-link-zip-task/?share_link_token=" + token + "&path=%2F&_=1570695690269")
    if r.ok:
        token = r.json()["zip_token"]

    r = requests.get("https://seafile.unistra.fr/seafhttp/zip/" + token, stream=True)
    with open("notes.zip", "wb") as file:
        for chunk in r:
            file.write(chunk)

    with ZipFile("notes.zip", "r") as zip_ref:
        for zipfile in zip_ref.infolist():
            if zipfile.filename[-1] == '/':
                continue
            zipfile.filename = os.path.basename(zipfile.filename)
            zip_ref.extract(zipfile, pdf_folder)
    os.remove("notes.zip")

def convert_pdf_to_list(path):
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

def process_pdfs():
    if not debug:
        mydb_create = mysql.connector.connect(host=host, user=login, passwd=passwd)
        cursor_create = mydb_create.cursor()
        cursor_create.execute("CREATE DATABASE IF NOT EXISTS `c1287446_main`")

        mydb = mysql.connector.connect(user=login, password=passwd, host=host, database="c1287446_main")
        cursor = mydb.cursor()
        sql = "CREATE DATABASE IF NOT EXISTS `c1287446_main`"
        cursor.execute(sql)
        sql = "SELECT count(*) FROM information_schema.TABLES WHERE (TABLE_SCHEMA = 'c1287446_main') AND (TABLE_NAME = 'global')"
        cursor.execute(sql)
        if list(cursor.fetchall()[0])[0] == 0:
            sql = "CREATE TABLE IF NOT EXISTS `global` (`id` int(255) NOT NULL KEY AUTO_INCREMENT,`type_note` varchar(255) NOT NULL,`type_epreuve` varchar(255) NOT NULL,`name_devoir` varchar(255) NOT NULL,`name_ens` varchar(255) NOT NULL,`name_pdf` varchar(255) NOT NULL,`link_pdf` varchar(255) NOT NULL,`note_code` varchar(255) NOT NULL,`note_coeff` int(8) NOT NULL,`note_semestre` varchar(255) NOT NULL,`note_date` date NOT NULL,`note_total` int(255) NOT NULL,`moy` double NOT NULL,`median` double NOT NULL,`mini` double NOT NULL,`maxi` double NOT NULL,`variance` double NOT NULL,`deviation` double NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=latin1;"
            cursor.execute(sql)
            records_global = []
        else:
            sql = "SELECT `name_pdf` FROM global"
            cursor.execute(sql)
            records_global = [x[0] for x in cursor.fetchall()]

    for filename in os.listdir(pdf_folder):
        list_el = [x for x in convert_pdf_to_list(pdf_folder + filename) if x != ""]

        type_note = list_el[list_el.index(msg_type_note) + 1]
        type_epreuve = list_el[list_el.index(msg_type_epr) + 1]
        name_devoir = list_el[list_el.index(msg_nom_module) + 1]
        name_ens = list_el[list_el.index(msg_nom_pren) + 1]
        link_pdf = "https://seafile.unistra.fr/d/" + token + "/files/?p=/" + filename + "&dl=1"
        name_pdf = link_pdf.split("/")[-1].split(".pdf")[0]
        y, m, d, _ = filename.split("_", 3)
        note_date = f"{y}-{m}-{d}"

        for main_key in subjects["MMI1"].keys():
            for x in subjects["MMI1"][main_key].keys():
                for y in name_pdf.split("_"):
                    if y == x:
                        note_code = y
                        note_coeff = subjects["MMI1"][main_key][y]
                        note_semestre = main_key
                        break

        etu_start_index = list_el.index("N° Etudiant")
        nb_etu = int(list_el[etu_start_index - 1])
        num_etu = list_el[etu_start_index + 1:etu_start_index + nb_etu + 1]
        note_start_index = list_el.index("Note")
        note_etu = list_el[note_start_index + 1:note_start_index + nb_etu + 1]

        clear_note_etu = [float(x.replace(",", ".")) for x in note_etu if x != " " and x.lower() != "abi" and x.lower() != "abs"]
        note_total = len(clear_note_etu)
        moy = statistics.mean(clear_note_etu)
        median = statistics.median(clear_note_etu)
        mini = min(clear_note_etu)
        maxi = max(clear_note_etu)
        variance = statistics.variance(clear_note_etu)
        deviation = statistics.stdev(clear_note_etu)

        dict_etu_note = list(zip(num_etu, note_etu))
        if debug:
            note_etu = [x[1] for x in dict_etu_note if x[0] == "21901316"][0]
            print(note_etu + "----------\n")
            print(type_note, type_epreuve, name_devoir, name_ens, name_pdf, link_pdf, note_code, note_coeff, note_semestre, note_date, note_total, moy, median, mini, maxi, variance, deviation)
            continue

        if name_pdf in records_global:
            print("'" + name_devoir + "' already in global.")
        else:
            print("Adding new line '" + name_devoir + "' in global.")
            sql = "INSERT INTO global (type_note, type_epreuve, name_devoir, name_ens, name_pdf, link_pdf, note_code, note_coeff, note_semestre, note_date, note_total, moy, median, mini, maxi, variance, deviation) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            val = (type_note, type_epreuve, name_devoir, name_ens, name_pdf, link_pdf, note_code, note_coeff, note_semestre, note_date, note_total, moy, median, mini, maxi, variance, deviation)
            cursor.execute(sql, val)

        sql = "SELECT count(*) FROM information_schema.TABLES WHERE (TABLE_SCHEMA = 'c1287446_main') AND (TABLE_NAME = '" + name_pdf + "')"
        cursor.execute(sql)
        if list(cursor.fetchall()[0])[0] == 0:
            print("Adding table '" + name_devoir + "'.")
            cursor.execute("CREATE TABLE IF NOT EXISTS `" + name_pdf + "` (`id_etu` int(8) NOT NULL,`note_etu` float NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=latin1;")
            for key, value in dict_etu_note:
                id_etu = int(key)
                note_etu = float(value.replace(",", ".")) if "," in value else 0
                sql = "INSERT INTO " + name_pdf + " (id_etu, note_etu) VALUES (%s, %s)"
                val = (id_etu, note_etu)
                cursor.execute(sql, val)
        else:
            print("'" + name_devoir + "' already exists.")

    if not debug:
        mydb.commit()
        mydb.close()

if __name__ == "__main__":
    main()
    process_pdfs()
