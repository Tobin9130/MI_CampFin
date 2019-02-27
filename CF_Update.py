import requests, os, zipfile, io, sqlite3
from bs4 import BeautifulSoup

root = os.getcwd()

source = 'http://miboecfr.nictusa.com/cfr/dumpall/cfrdetail/'
get = 'http://miboecfr.nictusa.com/cfr/dumpall/cfrdetail/'

response = requests.get(source)
soup = BeautifulSoup(response.text, 'lxml')
links = soup.find_all("a", href=True)
links = [str(get + x['href']) for x in links[5:-3]]
# links has only path to zipfiles

contributions = [x for x in links if 'contributions' in x]
expenditures = [x for x in links if 'expenditures' in x]
receipts = [x for x in links if 'receipts' in x]

def zipExtractor(input_list):
    for years in input_list:
        resp = requests.get(years)
        z = zipfile.ZipFile(io.BytesIO(resp.content))
        z.extractall()

#####################

db = sqlite3.connect('MichiganCF_DB.sqlite3')
cursor = db.cursor()
cursor.execute(''' CREATE TABLE contributions (doc_seq_no,page_no,contribution_id,cont_detail_id, doc_stmnt_year,doc_type_desc,com_legal_name,common_name, cfr_com_id,com_type,can_first_name,can_last_name,contribtype, f_name,l_name,address,city,state,zip,occupation,employer, received_date,amount,aggregate,extra_desc,RUNTIME)''') 

os.chdir(root+ '/Contributions')
zipExtractor(contributions)

files_c = os.listdir()

os.chdir(root)

errors = []
for n in files_c:
    with open(root + '/Contributions/' + n, 'r', encoding ='ISO-8859-1') as f:
        data = f.readlines()
    
    data = [x.split('\t') for x in data[1:]] #change to if len(x)<27]
    Good = [x for x in data if len(x) == 26]
    Bad = [x for x in data if len(x) != 26]
    
    cursor.executemany('INSERT INTO contributions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', Good)
    db.commit() 
    errors.append(Bad)

db.close()
    

#####################

os.chdir(root +'/Expenditures')
zipExtractor(expenditures)

files_c = os.listdir()

os.chdir(root)
db = sqlite3.connect('MichiganCF_DB.sqlite3')
cursor = db.cursor()
cursor.execute(''' CREATE TABLE expenditures(doc_seq_no,expenditure_type,gub_account_type,gub_elec_type,page_no,expense_id,detail_id,doc_stmnt_year,doc_type_desc,com_legal_name,common_name,cfr_com_id,com_type,schedule_desc,exp_desc,purpose,extra_desc,f_name,l_name,address,city,state,zip,exp_date,amount,state_loc,supp_opp,can_or_ballot,county,debt_payment,vend_name,vend_addr,vend_city,vend_state,vend_zip,gotv_ink_ind,fundraiser)''')


for n in files_c:
    with open(root +'/Expenditures/' + n, 'r', encoding ='ISO-8859-1') as f:
        data = f.readlines()
    
    data = [x.split('\t') for x in data[1:]] #change to if len(x)<40]
    Good = [x[:37] for x in data] #Belve 39
    
    cursor.executemany('INSERT INTO expenditures VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', Good)
    db.commit()


db.close()

#####################

os.chdir(root + '/Receipts')
zipExtractor(receipts)

files_c = os.listdir()

os.chdir(root)
db = sqlite3.connect('MichiganCF_DB.sqlite3')
cursor = db.cursor()
cursor.execute(''' CREATE TABLE receipts(doc_seq_no,ik_code,gub_account_type,gub_elec_type,page_no,receipt_id,detail_id,doc_stmnt_year,doc_type_desc,com_legal_name,common_name,cfr_com_id,com_type,can_first_name,can_last_name,contribtype,f_name,l_name,address,city,state,zip,occupation,employer,received_date,amount,aggregate,extra_desc,receipttype)''')

Master = []
for n in files_c:
    with open(root +'/Receipts/' + n, 'r', encoding ='ISO-8859-1') as f:
        data = f.readlines()
    
    data = [x.split('\t') for x in data[1:]] # Change to if len(x)==30]
    Good = [x[:29] for x in data] 
    
    cursor.executemany('INSERT INTO receipts VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', Good)
    db.commit()


db.close()
