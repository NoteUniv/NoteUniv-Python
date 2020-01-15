from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage
from zipfile import ZipFile
import os, json, time, requests, zipfile, io, dotenv, statistics, mysql.connector

# Load tokens + auth for mysql
dotenv.load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

verbose = True

env_tokens = {key: value for key, value in os.environ.items() if "MARKS_S1" in key}
host = os.environ.get("BDD_HOST")
login = os.environ.get("BDD_LOGIN")
passwd = os.environ.get("BDD_PASSWD")
bdd_name = "c1287446_main"

# Load subjects + coeffs
with open("subjects_coeff.json", "r", encoding="utf-8") as file:
    subjects = json.load(file)

def download_archives(name, val):
    # Get download token with classic token
    r = requests.get("https://seafile.unistra.fr/api/v2.1/share-link-zip-task/?share_link_token=" + val + "&path=/")
    if r.ok:
        token_pdf = r.json()["zip_token"]

    # Get marks using download token_pdf (cooldown to prepare file zipping)
    r = requests.get("https://seafile.unistra.fr/seafhttp/zip/" + token_pdf, time.sleep(1))
    # Download as stream (works better)
    with open(name + ".zip", "wb") as file:
        for chunk in r:
            file.write(chunk)

def unzip_archives(name):
    global is_empty
    # Open all zip files and extract them
    with ZipFile(name + ".zip", "r") as zip_ref:
        for zipfile in zip_ref.infolist():
            if zipfile.filename[-1] == '/':
                continue
            zipfile.filename = os.path.basename(zipfile.filename)
            zip_ref.extract(zipfile, name)
    is_empty = False
    if not os.path.exists(name):
        os.makedirs(name)
        is_empty = True
    if not os.listdir(name):
        is_empty = True
    os.remove(name + ".zip")

def convert_pdf_to_list(path):
    # Writing in StringIO doc to store pdf text as list
    output = io.StringIO()
    manager = PDFResourceManager()
    converter = TextConverter(manager, output, laparams=LAParams())
    interpreter = PDFPageInterpreter(manager, converter)

    infile = open(path, "rb")
    for page in PDFPage.get_pages(infile, {0}):
        interpreter.process_page(page)
    infile.close()
    converter.close()
    text = output.getvalue()
    output.close()
    return text.split("\n")

def handle_db(name, sem):
    global db_noteuniv, noteuniv_cursor, records_global, rows_complete, tables_complete
    # Create main database if not exists
    db_noteuniv1 = mysql.connector.connect(host=host, user=login, passwd=passwd)
    noteuniv_cursor1 = db_noteuniv1.cursor()
    if verbose:
        print("Creating database if not exists.")
    noteuniv_cursor1.execute("CREATE DATABASE IF NOT EXISTS `" + bdd_name + "`")
    db_noteuniv1.commit()
    db_noteuniv1.close()

    # Login to this database directly
    db_noteuniv = mysql.connector.connect(user=login, password=passwd, host=host, database=bdd_name)
    noteuniv_cursor = db_noteuniv.cursor()
    # Check if global table exists
    sql = "SELECT count(*) FROM information_schema.TABLES WHERE (TABLE_SCHEMA = '" + bdd_name + "') AND (TABLE_NAME = 'global_" + sem + "')"
    noteuniv_cursor.execute(sql)
    if list(noteuniv_cursor.fetchall()[0])[0] == 0:
        # Create table shema
        if verbose:
            print("Creating global_" + sem + " database.")
        sql = "CREATE TABLE IF NOT EXISTS `global_" + sem + "` (`id` int(255) NOT NULL KEY AUTO_INCREMENT,`type_note` varchar(255) NOT NULL,`type_epreuve` varchar(255) NOT NULL,`name_devoir` varchar(255) NOT NULL,`name_ens` varchar(255) NOT NULL,`name_pdf` varchar(255) NOT NULL,`link_pdf` varchar(255) NOT NULL,`note_code` varchar(255) NOT NULL,`note_coeff` int(8) NOT NULL,`note_semester` varchar(255) NOT NULL,`note_date` date NOT NULL,`note_total` int(255) NOT NULL,`moy` double NOT NULL,`median` double NOT NULL,`mini` double NOT NULL,`maxi` double NOT NULL,`variance` double NOT NULL,`deviation` double NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8;"
        noteuniv_cursor.execute(sql)
        records_global = []
        rows_complete = False
        tables_complete = False
    else:
        # Select all data from global table
        sql = "SELECT `name_pdf` FROM `global_" + sem + "`"
        noteuniv_cursor.execute(sql)
        records_global = [x[0] for x in noteuniv_cursor.fetchall()]

        # Check if rows in global == pdf count
        if len(records_global) == len(os.listdir(name)):
            rows_complete = True
        else:
            rows_complete = False

        # Check if all PDFs are in all tables
        sql = "SELECT `TABLE_NAME` FROM information_schema.TABLES WHERE (TABLE_SCHEMA = '" + bdd_name + "')"
        noteuniv_cursor.execute(sql)
        all_tables = [x[0] for x in noteuniv_cursor.fetchall()]
        if all([x.split(".pdf")[0] in all_tables for x in os.listdir(name)]):
            tables_complete = True
        else:
            tables_complete = False

        # Exit if nothing to update (avoid useless requests)
        if rows_complete and tables_complete:
            exit("Nothing more to add, tables and global are not updated.")

def process_pdfs(name, sem):
    global db_noteuniv, noteuniv_cursor, name_pdf
    # Loop PDF files
    for filename in os.listdir(name):
        # Get all data from PDF (list)
        list_el = convert_pdf_to_list(name + "/" + filename)

        # Get main infos with text indexes
        msg_type_note = [x for x in list_el if "type de note" in x.lower()][0]
        type_note = list_el[list_el.index(msg_type_note) + 1]
        msg_type_epreuve = [x for x in list_el if "type d'épreuve" in x.lower()][0]
        type_epreuve = list_el[list_el.index(msg_type_epreuve) + 1]
        msg_nom_devoir = [x for x in list_el if "nom du devoir" in x.lower()][0]
        name_devoir = list_el[list_el.index(msg_nom_devoir) + 1]
        msg_name_ens = [x for x in list_el if "enseignant" in x.lower()][0]
        name_ens = list_el[list_el.index(msg_name_ens) + 1]

        # Get other infos about mark
        link_pdf = "https://seafile.unistra.fr/d/token/files/?p=/" + filename + "&dl=1"
        name_pdf_raw = link_pdf.split("/")[-1].split(".pdf")[0]
        name_pdf = name_pdf_raw.replace(" ", "_")[:64]
        y, m, d, _ = filename.split("_", 3)
        note_date = f"{y}-{m}-{d}"

        # Loop keys to know code and coeff
        for main_key in subjects[sem].keys():
            for x in subjects[sem][main_key].keys():
                for y in name_pdf.split("_"):
                    if y.lower() == x.lower():
                        note_code = y
                        note_coeff = subjects[sem][main_key][y]
                        note_semester = main_key
                        break

        # Check format of PDF, blank is useless if space in doc
        if " " in list_el:
            list_el = [x for x in list_el if x != ""]
            shift = 1
        else:
            # Blank is useless if PDF contains
            if any([x.lower() in ["abi", "abs"] for x in list_el]):
                list_el = [x for x in list_el if x != ""]
                shift = 1
            # Blank mean ABS if not abs or abi is not mentionned
            else:
                list_el = [x if x != "" else "ABS" for x in list_el]
                shift = 2

        msg_etu_index = [x for x in list_el if "etudiant" in x.lower()][-1]
        etu_start_index = list_el.index(msg_etu_index)
        msg_note_index = [x for x in list_el if "maximale" in x.lower()][-1]
        note_start_index = list_el.index(msg_note_index) + 3 # index de maximale

        # Get lists of all num etu and all marks
        nb_etu = int(list_el[etu_start_index - shift])
        num_etu = list_el[etu_start_index + 1:etu_start_index + nb_etu + 1]
        note_etu = list_el[note_start_index + 1:note_start_index + nb_etu + 1]

        # Calculate many stats from marks
        clear_note_etu = [float(x.replace(",", ".")) for x in note_etu if "," in x]
        note_total = len(clear_note_etu)
        moy = statistics.mean(clear_note_etu)
        median = statistics.median(clear_note_etu)
        mini = min(clear_note_etu)
        maxi = max(clear_note_etu)
        variance = statistics.variance(clear_note_etu)
        deviation = statistics.stdev(clear_note_etu)

        # All ABS are set to 100 (handled by website)
        note_etu = [float(x.replace(",", ".")) if "," in x else "100" for x in note_etu]

        # Gen a dict with ids and marks merged
        dict_etu_note = list(zip(num_etu, note_etu))

        # Test if line exists in global
        if name_pdf in records_global:
            if verbose:
                print("'" + name_devoir + "' already in global.")
        else:
            if verbose:
                print("Adding new line '" + name_devoir + "' in global.")
            sql = "INSERT INTO global_" + sem + " (type_note, type_epreuve, name_devoir, name_ens, name_pdf, link_pdf, note_code, note_coeff, note_semester, note_date, note_total, moy, median, mini, maxi, variance, deviation) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            val = (type_note, type_epreuve, name_devoir, name_ens, name_pdf, link_pdf, note_code, note_coeff, note_semester, note_date, note_total, moy, median, mini, maxi, variance, deviation)
            noteuniv_cursor.execute(sql, val)

        # Test if table exists
        sql = "SELECT count(*) FROM information_schema.TABLES WHERE (TABLE_SCHEMA = '" + bdd_name + "') AND (TABLE_NAME = '" + name_pdf + "')"
        noteuniv_cursor.execute(sql)
        if list(noteuniv_cursor.fetchall()[0])[0] == 0:
            if verbose:
                print("Adding table '" + name_devoir + "'.")
            noteuniv_cursor.execute("CREATE TABLE IF NOT EXISTS `" + name_pdf + "` (`id_etu` int(8) NOT NULL,`note_etu` float NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
            all_data = []
            for id_etu, note_etu in dict_etu_note:
                all_data.append((id_etu, note_etu))
            sql = "INSERT INTO `" + name_pdf + "` (id_etu, note_etu) VALUES (%s, %s)"
            noteuniv_cursor.executemany(sql, all_data)
        else:
            if verbose:
                print("'" + name_devoir + "' already exists.")

def update_ranking():
    # Check if global table exists
    sql = "SELECT count(*) FROM information_schema.TABLES WHERE (TABLE_SCHEMA = '" + bdd_name + "') AND (TABLE_NAME = 'ranking_" + sem + "')"
    noteuniv_cursor.execute(sql)
    if list(noteuniv_cursor.fetchall()[0])[0] == 0:
        # Create table shema
        if verbose:
            print("Creating ranking_" + sem + " database.")
        sql = "CREATE TABLE IF NOT EXISTS `ranking_" + sem + "` (`id_etu` int(8) NOT NULL,`moy_etu` float NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8;"
        noteuniv_cursor.execute(sql)
    else:
        # Clear all table
        if verbose:
            print("Cleaning up ranking_" + sem + " database.")
        sql = "TRUNCATE TABLE `ranking_" + sem + "`"
        noteuniv_cursor.execute(sql)
    if is_empty:
        return

    # Get all id_etu from any PDF file (latest processed)
    sql = "SELECT `id_etu` FROM `" + name_pdf + "`"
    noteuniv_cursor.execute(sql)
    if verbose:
        print("Updating ranking...")
    all_data = []
    for id_etu in noteuniv_cursor.fetchall():
        sql = "SELECT `name_pdf`, `mini`, `note_coeff`, `type_note` FROM `global_" + sem + "`"
        noteuniv_cursor.execute(sql)
        all_notes = []
        all_coeff = []
        # Get all etu notes from all PDFs
        for note_data in noteuniv_cursor.fetchall():
            sql = "SELECT `note_etu` FROM " + str(note_data[0]) + " WHERE id_etu = '" + str(id_etu[0]) + "'"
            noteuniv_cursor.execute(sql)
            note_etu_mark = noteuniv_cursor.fetchall()
            # Insert notes and coeffs to lists
            if list(note_etu_mark[0])[0] < 21 and any([x in note_data[3] for x in ["Note unique", "Moyenne de notes"]]):
                note_etu_mark_coeff = note_data[2]
                note_etu_mark_final = note_etu_mark[0] * note_etu_mark_coeff
                all_notes.append(note_etu_mark_final)
                all_coeff.append(note_etu_mark_coeff)

        # Weighted average on all marks for etu
        moy_etu = sum([sum(x) for x in all_notes]) / sum(all_coeff)
        # Insert average for each etu
        all_data.append((id_etu[0], round(moy_etu, 2)))
    sql = "INSERT INTO `ranking_" + sem + "` (id_etu, moy_etu) VALUES (%s, %s)"
    noteuniv_cursor.executemany(sql, all_data)

if __name__ == "__main__":
    # Start main function and then process PDFs + DB push
    for key, val in env_tokens.items():
        name = key.lower()
        sem = name.split("_")[-1]
        download_archives(name, val)
        unzip_archives(name)
        handle_db(name, sem)
        process_pdfs(name, sem)
        if not tables_complete:
            update_ranking()
        # Commit changes (push)
        db_noteuniv.commit()
        db_noteuniv.close()
    if verbose:
        print("Everything has been successfully updated!")
