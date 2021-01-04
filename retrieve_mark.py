from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage
import os, shutil, json, time, requests, zipfile, io, dotenv, statistics, mysql.connector
from token_cipher import decipher

# Load tokens + auth for mysql
dotenv.load_dotenv(".env")

verbose = True # Print debug infos
records_global = [] # List of all PDF name and size
name_pdf = "" # Global for latest processed file for ranking
list_pdf_changed = [] # Which PDF has been chaged since last update
rows_complete = False # Is global table complete
tables_complete = False # Is database tables complete
is_empty = False # Semester folder empty

env_tokens = {key: value for key, value in os.environ.items() if "MARKS_S" in key}
host = os.environ.get("BDD_HOST")
bdd_name = os.environ.get("BDD_NAME")
login = os.environ.get("BDD_LOGIN")
passwd = os.environ.get("BDD_PASSWD")
webhook_url_1 = os.environ.get("WEBHOOK_URL_1")
webhook_url_2 = os.environ.get("WEBHOOK_URL_2")

# Load subjects + coefficients
with open("subjects_coeff.json", "r", encoding="utf-8") as file:
    subjects = json.load(file)

def to_name(thing):
    return thing.split("/")[-1].split(".pdf")[0].replace(" ", "_")[:64].lower()

def download_archive(sem_name, sem_token):
    # Get download token with global token
    r = requests.get("https://seafile.unistra.fr/api/v2.1/share-link-zip-task/?share_link_token=" + sem_token + "&path=/")
    if r.ok:
        token_pdf = r.json()["zip_token"]
    else:
        exit()

    # Send a query to compress every PDF
    while True:
        r = requests.get("https://seafile.unistra.fr/api/v2.1/query-zip-progress/?token=" + token_pdf)
        # Request returns a JSON with number of zipped files and total files
        if r.json()["zipped"] != r.json()["total"]:
            # print("Not fully zipped yet")
            time.sleep(2)
        else:
            break

    r = requests.get("https://seafile.unistra.fr/seafhttp/zip/" + token_pdf)
    # Download as stream and write bytes (works better)
    with open(sem_name + ".zip", "wb") as file:
        for chunk in r:
            file.write(chunk)

def unzip_archive(sem_name):
    global is_empty
    # Open all zip files and extract them
    with zipfile.ZipFile(sem_name + ".zip", "r") as zip_ref:
        for zip_file in zip_ref.infolist():
            # If zip file contains a folder
            if zip_file.filename[-1] == "/":
                continue
            zip_file.filename = os.path.basename(zip_file.filename.encode("cp437").decode("utf8"))
            zip_ref.extract(zip_file, sem_name)
    if not os.path.exists(sem_name):
        os.makedirs(sem_name)
        is_empty = True
    if not os.listdir(sem_name):
        is_empty = True
    os.remove(sem_name + ".zip")

def convert_pdf_to_list(path):
    # Writing in StringIO doc to store pdf text as list
    output = io.StringIO()
    manager = PDFResourceManager()
    converter = TextConverter(manager, output, laparams=LAParams())
    interpreter = PDFPageInterpreter(manager, converter)

    file = open(path, "rb")
    for page in PDFPage.get_pages(file, {0}):
        interpreter.process_page(page)
    file.close()
    converter.close()
    text = output.getvalue()
    output.close()
    return text.split("\n")

def handle_db(sem_name, sem):
    global records_global, rows_complete, tables_complete
    # Check if global table exists
    sql = "SELECT count(*) FROM information_schema.TABLES WHERE (TABLE_SCHEMA = '" + bdd_name + "') AND (TABLE_NAME = 'global_" + sem + "')"
    noteuniv_cursor.execute(sql)
    rows_complete = False
    tables_complete = False
    if list(noteuniv_cursor.fetchall()[0])[0] == 0:
        # Create table schema
        if verbose:
            print("Creating global_" + sem + " table.")
        sql = "CREATE TABLE IF NOT EXISTS `global_" + sem + "` (`id` int NOT NULL KEY AUTO_INCREMENT,`type_note` varchar(255) NOT NULL,`type_exam` varchar(255) NOT NULL,`name_note` varchar(255) NOT NULL,`name_teacher` varchar(255) NOT NULL,`name_pdf` varchar(255) NOT NULL,`link_pdf` varchar(255) NOT NULL,`size_pdf` int NOT NULL,`note_code` varchar(63) NOT NULL,`note_semester` varchar(63) NOT NULL,`note_date_c` date NOT NULL,`note_date_m` timestamp NOT NULL,`note_coeff` tinyint NOT NULL,`note_total` tinyint NOT NULL,`average` double NOT NULL,`median` double NOT NULL,`minimum` double NOT NULL,`maximum` double NOT NULL,`variance` double NOT NULL,`deviation` double NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8;"
        noteuniv_cursor.execute(sql)
    else:
        # Select all data from global table
        sql = "SELECT `name_pdf`, `size_pdf` FROM `global_" + sem + "`"
        noteuniv_cursor.execute(sql)
        records_global = noteuniv_cursor.fetchall()

        # Check if rows in global == pdf count
        if len(records_global) == len([x for x in os.listdir(sem_name) if x.startswith("20") and x.endswith(".pdf")]):
            rows_complete = True

        # Check if all PDF are in all tables
        sql = "SELECT `TABLE_NAME` FROM information_schema.TABLES WHERE (TABLE_SCHEMA = '" + bdd_name + "')"
        noteuniv_cursor.execute(sql)
        all_tables = [x[0] for x in noteuniv_cursor.fetchall()]
        if all([to_name(x) in [to_name(y) for y in all_tables] for x in os.listdir(sem_name) if to_name(x).startswith("20")]):
            tables_complete = True

def send_webhook(sem, note_code, name_teacher, name_note, type_note, type_exam, note_date_c, average):
    # JSON webhook for discord message
    webhook_data = {
        "username": "NoteUniv",
        "avatar_url": "https://noteuniv.fr/assets/images/logo_rounded.png",
        "embeds": [
            {
                "title": f"Nouvelle note de {note_code} sur NoteUniv !",
                "description": "Une nouvelle note a été publiée il y a peu, allez la voir sur le site web !",
                "url": "https://noteuniv.fr",
                "color": 1114419,
                "thumbnail": {
                    "url": "https://noteuniv.fr/assets/images/logo_rounded.png"
                },
                "fields": [
                    {
                        "name": "Enseignant :",
                        "value": name_teacher if name_teacher != "" else "❌",
                        "inline": True
                    },
                    {
                        "name": "Devoir :",
                        "value": name_note if name_note != "" else "❌",
                        "inline": True
                    },
                    {
                        "name": "Type de note :",
                        "value": type_note if type_note != "" else "❌",
                        "inline": True
                    },
                    {
                        "name": "Type épreuve :",
                        "value": type_exam if type_exam != "" else "❌",
                        "inline": True
                    },
                    {
                        "name": "Date :",
                        "value": note_date_c if note_date_c != "" else "❌",
                        "inline": True
                    },
                    {
                        "name": "Moyenne :",
                        # Need to convert to string!
                        "value": str(round(average, 2)),
                        "inline": True
                    }
                ],
                "footer": {
                    "icon_url": "https://noteuniv.fr/assets/images/noteuniv_logo.jpg",
                    "text": "Merci d'utiliser NoteUniv ♥"
                }
            }
        ]
    }

    # Send a webhook in the correct channel for every MMI
    if sem == "s1" or sem == "s2":
        requests.post(webhook_url_1, json=webhook_data)
    elif sem == "s3" or sem == "s4":
        requests.post(webhook_url_2, json=webhook_data)

def send_notification(sem, note_code, name_teacher, name_note, note_date_c, average):
    promo = (int(sem[-1]) + 2 - 1) // 2
    noteuniv_cursor.execute("SELECT token, key_token FROM data_etu WHERE promo = 'MMI" + str(promo) + "'")

    all_tokens = []

    for token, key_token in noteuniv_cursor.fetchall():
        if token:
            all_tokens.append(decipher(token, key_token))

    notification_data = {
        "to": all_tokens,
        "title": f"🎓 Nouvelle note en {note_code} de {name_teacher}",
        "body": f"ℹ️ {name_note}\n📅 Date : {note_date_c}\n📈 Moyenne : {round(average, 2)}",
        "priority": "high"
    }

    # Send the notification to all students of the promo
    requests.post("https://exp.host/--/api/v2/push/send", json=notification_data)

def process_pdf(sem_name, sem, sem_token):
    global name_pdf, list_pdf_changed
    # Loop PDF files
    for filename in [x for x in os.listdir(sem_name) if x.startswith("20") and x.endswith(".pdf")]: # Exclude other formats
        # Get all data from PDF (list)
        list_el = convert_pdf_to_list(sem_name + "/" + filename)

        if verbose:
            print("Processing '" + filename + "'.")

        # Get main infos with text indexes
        msg_type_note = [x for x in list_el if "type de note" in x.lower()][0]
        type_note = list_el[list_el.index(msg_type_note) + 1]
        msg_type_exam = [x for x in list_el if "type d'épreuve" in x.lower()][0]
        type_exam = list_el[list_el.index(msg_type_exam) + 2]
        msg_nom_note = [x for x in list_el if "nom du devoir" in x.lower()][0]
        name_note = list_el[list_el.index(msg_nom_note) + 1]
        if not name_note:
            name_note = list_el[list_el.index(msg_nom_note) + 2]
        msg_name_teacher = [x for x in list_el if "enseignant" in x.lower()][0]
        name_teacher = list_el[list_el.index(msg_name_teacher) + 1]

        # Get other infos about mark
        link_pdf = "https://seafile.unistra.fr/d/" + sem_token + "/files/?p=/" + filename
        name_pdf = to_name(link_pdf)
        size_pdf = os.stat(sem_name + "/" + filename).st_size
        y, m, d, _ = filename.split("_", 3)
        if len(y) != 4:
            y = time.strftime("%Y")
        note_date_c = f"{y}-{m}-{d}"
        note_date_m = time.strftime("%y-%m-%d %H:%M:%S", time.gmtime(os.stat(sem_name + "/" + filename).st_atime))

        # Loop keys to know code and coeff
        for main_key in subjects[sem].keys():
            for x in subjects[sem][main_key].keys():
                for y in name_pdf.split("_"):
                    if y.lower() == x.lower():
                        note_code = y
                        note_coeff = subjects[sem][main_key][y.upper()]
                        note_semester = main_key
                        break

        # Check format of PDF, blank is useless if space in doc
        list_el = [x for x in list_el if x != ""]

        msg_etu_index = [x for x in list_el if "etudiant" in x.lower()][-1]
        etu_start_index = list_el.index(msg_etu_index)
        msg_note_index = [x for x in list_el if "Note" in x][-1]
        note_start_index = list_el.index(msg_note_index)

        # Get lists of all num etu and all marks
        nb_etu = int(list_el[etu_start_index - 1])
        num_etu = list_el[etu_start_index + 1:etu_start_index + nb_etu + 1]
        note_etu = list_el[note_start_index + 1:note_start_index + nb_etu + 1]
        # If PDF spaces are broken
        if nb_etu != len(note_etu):
            note_etu = ["100,000"] * nb_etu

        # Calculate many stats from marks
        clear_note_etu = [float(x.replace(",", ".")) for x in note_etu if "," in x]
        note_total = len(clear_note_etu)
        average = statistics.mean(clear_note_etu)
        median = statistics.median(clear_note_etu)
        minimum = min(clear_note_etu)
        maximum = max(clear_note_etu)
        variance = statistics.variance(clear_note_etu)
        deviation = statistics.stdev(clear_note_etu)

        # All ABS are set to 100 (handled by website)
        note_etu = [float(x.replace(",", ".")) if "," in x else "100" for x in note_etu]

        # Gen a dict with ids and marks merged
        dict_etu_note = list(zip(num_etu, note_etu))

        # Test if line exists in global
        if name_pdf in [x[0] for x in records_global]:
            if verbose:
                print("'" + name_note + "' already in global.")
            # Check if this PDF changed
            if os.stat(sem_name + "/" + filename).st_size != dict(records_global)[to_name(filename)]:
                list_pdf_changed.append(to_name(filename))
            if name_pdf in list_pdf_changed:
                print("'" + name_note + "' needs to be updated for new marks.")
                sql = "UPDATE global_" + sem + " SET size_pdf = %s, note_date_m = %s, note_total = %s, average = %s, median = %s, minimum = %s, maximum = %s, variance = %s, deviation = %s WHERE name_pdf = %s"
                sql_data = (size_pdf, note_date_m, note_total, average, median, minimum, maximum, variance, deviation, name_pdf)
                noteuniv_cursor.execute(sql, sql_data)
        else:
            if verbose:
                print("Adding new line '" + name_note + "' in global.")
            sql = "INSERT INTO global_" + sem + " (type_note, type_exam, name_note, name_teacher, name_pdf, link_pdf, size_pdf, note_code, note_coeff, note_semester, note_date_c, note_date_m, note_total, average, median, minimum, maximum, variance, deviation) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            global_data = (type_note, type_exam, name_note, name_teacher, name_pdf, link_pdf, size_pdf, note_code, note_coeff, note_semester, note_date_c, note_date_m, note_total, average, median, minimum, maximum, variance, deviation)
            noteuniv_cursor.execute(sql, global_data)

            # Send a discord webhook for every mark
            send_webhook(sem, note_code, name_teacher, name_note, type_note, type_exam, note_date_c, average)
            # Send a notification on user's device
            send_notification(sem, note_code, name_teacher, name_note, note_date_c, average)

        # Test if table exists
        sql = "SELECT count(*) FROM information_schema.TABLES WHERE (TABLE_SCHEMA = '" + bdd_name + "') AND (TABLE_NAME = '" + name_pdf + "')"
        noteuniv_cursor.execute(sql)
        if list(noteuniv_cursor.fetchall()[0])[0] == 0:
            if verbose:
                print("Adding table '" + name_note + "'.")
            noteuniv_cursor.execute("CREATE TABLE IF NOT EXISTS `" + name_pdf + "` (`id_etu` int NOT NULL,`note_etu` float NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
            sql_data = []
            for id_etu, note_etu in dict_etu_note:
                sql_data.append((id_etu, note_etu))
            sql = "INSERT INTO `" + name_pdf + "` (id_etu, note_etu) VALUES (%s, %s)"
            noteuniv_cursor.executemany(sql, sql_data)
        else:
            if verbose:
                print("'" + name_note + "' already exists.")

if __name__ == "__main__":
    # Create main database if not exists
    db_noteuniv1 = mysql.connector.connect(host=host, user=login, passwd=passwd)
    noteuniv_cursor1 = db_noteuniv1.cursor()
    if verbose:
        print("Creating database " + bdd_name + " if not exists.")
    noteuniv_cursor1.execute("CREATE DATABASE IF NOT EXISTS `" + bdd_name + "`")
    noteuniv_cursor1.close()
    db_noteuniv1.commit()
    db_noteuniv1.close()

    # Start main function and then process PDF + DB push
    for sem_code, sem_token in env_tokens.items():
        sem_name = sem_code.lower()
        sem = sem_name.split("_")[-1]
        download_archive(sem_name, sem_token)
        unzip_archive(sem_name)
        # Login to this database directly (every semester for connection lost)
        db_noteuniv = mysql.connector.connect(user=login, password=passwd, host=host, database=bdd_name)
        noteuniv_cursor = db_noteuniv.cursor()
        handle_db(sem_name, sem)
        # Continue if nothing to update (avoid useless requests)
        if rows_complete and tables_complete and not list_pdf_changed:
            if verbose:
                print("Nothing more to add, tables and global will not be updated.")
        else:
            process_pdf(sem_name, sem, sem_token)
            # Commit changes (push)
            db_noteuniv.commit()
            print("Everything has been successfully updated!")
            # Send request to update ranking
            url_ranking = os.environ.get("URL_RANKING")
            if url_ranking:
                params = {"action": "updateRanking",
                          "semestre": sem[-1]}
                requests.post(url_ranking, data=params)
        # Delete old folders to remove fail marks
        shutil.rmtree(sem_name, ignore_errors=True)
        noteuniv_cursor.close()
        db_noteuniv.close()
