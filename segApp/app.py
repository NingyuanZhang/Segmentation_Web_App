import numpy as np
import os
import pandas as pd
from myProject import app,db
from flask import render_template, redirect, request, url_for, flash,abort
from sqlalchemy import create_engine
from flask_login import login_user,login_required,logout_user
from myProject.forms import LoginForm, RegistrationForm
from myProject.extraction import GetData
from myProject.process import Process
from werkzeug.security import generate_password_hash, check_password_hash
import time
import random
from werkzeug import secure_filename

engine = create_engine('postgresql+psycopg2://jpvbhrhuoebcpa:85132a424093e5af005ebdf475ac57a7e862abaf0637d5e451c4130cf939aa62@ec2-18-210-51-239.compute-1.amazonaws.com:5432/ddqdiai90m75jm', echo = False)

@app.route('/', methods=['GET'])
def home():
    # create a new table to store transcodification
    #query0 = "CREATE TABLE TransCod(trans_id CHAR(15),company_id CHAR(50),line_id CHAR(50),cont_id CHAR(50),cont_initial_contact_date CHAR(50),cont_status CHAR(50),cont_segment CHAR(50),cont_civility CHAR(50),cont_firstname CHAR(50),cont_lastname CHAR(50),cont_title CHAR(50),cont_email CHAR(50),cont_cellphone CHAR(50),cont_comment CHAR(50),cont_residency_state CHAR(50),cont_residency_street CHAR(50),cont_residency_street_compl01 CHAR(50),cont_residency_city CHAR(50),cont_residency_zip CHAR(50),cont_company_name CHAR(50),cont_company_state CHAR(50),cont_company_street CHAR(50),cont_company_city CHAR(50),cont_company_zip CHAR(50),transaction_id CHAR(50),transaction_date CHAR(50),transaction_amount_tax CHAR(50),transaction_amount_net CHAR(50),payment_date CHAR(50),delivery_date CHAR(50),prod_id CHAR(50),prod_price_tax CHAR(50),prod_price_net CHAR(50),prod_family CHAR(50),prod_subfamily CHAR(50),prod_category CHAR(50),prod_name CHAR(50),prod_style CHAR(50),prod_collection CHAR(50),prod_fabric CHAR(50),prod_color CHAR(50),sales_id CHAR(50),sales_firstname CHAR(50),sales_lastname CHAR(50),sales_title CHAR(50),sales_email CHAR(50),store_id CHAR(50),store_name CHAR(50),store_manager_flag CHAR(50),store_address CHAR(50),store_phone CHAR(50),store_city CHAR(50),store_state CHAR(50),date_of_record CHAR(50),state_of_stock_01 CHAR(50),state_of_stock_02 CHAR(50),other01 CHAR(50),other02 CHAR(50),other03 CHAR(50),other04 CHAR(50),other05 CHAR(50),other06 CHAR(50),other07 CHAR(50),other08 CHAR(50),other09 CHAR(50),other10 CHAR(50),PRIMARY KEY(Trans_ID))"
    #engine.execute(query0)
    return render_template('home.html')



@app.route('/upload', methods=['POST'])
def upload_file():
    file = request.files['file']
    filename = secure_filename(file.filename)
    global oriName
    oriName = filename
    #filename = "client.csv"
    file.save("myProject/static/file/upload/client.csv")
    file.save("myProject/static/file/client.csv")
    #flash("Upload Success!")
    # read csv file as dataframe
    #csv_data_df = pd.read_csv("myProject/static/file/upload/client.csv", sep='\t')
    # get the file name as the new table name
    #tableSQL = filename.rsplit('.', 1)[0]
    # save the dataframe to database
    #csv_data_df.to_sql(name = tableSQL, con = engine,chunksize=1000, if_exists = 'replace', index = False)
    return redirect(url_for('matchCol'))

@app.route('/matchCol',methods=['GET','POST'])
def matchCol():

    dfMaster = pd.read_csv("myProject/static/file/master.csv")
    dfClient = pd.read_csv("myProject/static/file/upload/client.csv", sep='\t')
    #dfClient = pd.read_csv("myProject/static/file/client.csv")
    # get columns in client table
    data = []
    vals = dfClient.values
    # split on delimitator '|'
    for row in vals:
        strs = row[0].split('|')
        data.append(strs)
        # generate a new dataframe
    cols = dfClient.columns.values[0].split('|')
    dfClient = pd.DataFrame(data, columns=cols)
    client_label = cols
    # get columns in master table
    data = []
    vals = dfMaster.values
    for row in vals:
        strs = row[0].split('|')
        data.append(strs)
    cols = dfMaster.columns.values[0].split('|')
    dfMaster = pd.DataFrame(data, columns=cols)
    master_label = list(dfMaster['master_label'])
    p = Process()

    mapping_dict = p.buildDict(master_label)
    mapping_res = p.match(client_label,master_label)
    return render_template('match.html',length=len(client_label),mapping_res=mapping_res,master_label=master_label,client_label=client_label,name=oriName)


@app.route('/merge',methods=['POST'])
def merge():
    #select = request.form.get('method_cipher')
    dfMaster = pd.read_csv("myProject/static/file/master.csv")
    dfClient = pd.read_csv("myProject/static/file/upload/client.csv", sep='\t')
    # get columns in client table
    data = []
    vals = dfClient.values
    for row in vals:
        strs = row[0].split('|')
        data.append(strs)
    client_label = dfClient.columns.values[0].split('|')
    dfClient = pd.DataFrame(data, columns=client_label)
    #the original columns in client table
    oldCols = list(dfClient.columns)
    # get columns in master table
    data = []
    vals = dfMaster.values
    for row in vals:
        strs = row[0].split('|')
        data.append(strs)
    cols = dfMaster.columns.values[0].split('|')
    dfMaster = pd.DataFrame(data, columns=cols)
    master_label = list(dfMaster['master_label'])
    # get data from front-end
    keys = list(request.form.keys())
    # get matched columns
    newCols = []
    for key in keys:
        if key!='submit':
            #res_dict[key] = request.form.get(key)
            newCols.append(request.form.get(key))
    # create the matched table
    dfMerge = dfClient.copy()
    dfMerge.columns = newCols
    dfMerge.to_csv("myProject/static/file/matched.csv",index = False)

    #get Trans_ID
    timeStamp = int(time.time())
    randNum = random.randint(0,9999)+10000
    trans_id = str(timeStamp)+str(randNum)

    data = []
    oldCols.insert(0,'SampleID')
    oldCols.insert(0,trans_id)
    data.append(oldCols)
    newCols.insert(0,'company_id')
    newCols.insert(0,'trans_id')
    dfMatch = pd.DataFrame(data,columns = newCols)
    dfMatch.to_csv("myProject/static/file/CurTransCod.csv")

    query = "INSERT INTO TransCod ("
    for i in range(len(newCols)):
        query += newCols[i]
        if(i<len(newCols)-1):
            query += ","
    query += ") VALUES ("
    for i in range(len(oldCols)):
        query = query + "'" + oldCols[i] + "'"
        if(i<len(newCols)-1):
            query += ","
    query += ")"

    #print("______________query: "+query)

    engine.execute(query)

    return redirect((url_for('seg')))

@app.route("/downloadCSV",methods=['POST'])
def downloadCSV():
    dfMaster = pd.read_csv("myProject/static/file/master.csv")
    dfClient = pd.read_csv("myProject/static/file/upload/client.csv", sep='\t')
    # get columns in client table
    data = []
    vals = dfClient.values
    for row in vals:
        strs = row[0].split('|')
        data.append(strs)
    client_label = dfClient.columns.values[0].split('|')
    dfClient = pd.DataFrame(data, columns=client_label)
    # get columns in master table
    data = []
    vals = dfMaster.values
    for row in vals:
        strs = row[0].split('|')
        data.append(strs)
    cols = dfMaster.columns.values[0].split('|')
    dfMaster = pd.DataFrame(data, columns=cols)
    keys = list(request.form.keys())
    res_dict = {}
    newCols = []
    for key in keys:
        if key!='submit':
            res_dict[key] = request.form.get(key)
            newCols.append(request.form.get(key))
    dfMerge = dfClient.copy()
    dfMerge.columns = newCols

    dfMerge.to_csv("myProject/static/file/matched.csv",index = False)
    return GetData(dfMerge)()

@app.route("/seg",methods=['GET','POST'])
def seg():
    return render_template('seg.html',filename = "matched.csv");


@app.route("/seg/cus_seg",methods=['GET','POST'])
def cus_seg():

    dfMerged = pd.read_csv('myProject/static/file/matched.csv')
    p = Process()
    cols = p.getCusCols(dfMerged)
    return render_template('cus_seg.html',cols=cols,length = len(cols),filename = "matched.csv");


@app.route("/seg/cus_seg_res",methods=['GET','POST'])
def cus_seg_res():
    dfMerged = pd.read_csv('myProject/static/file/matched.csv',dtype='str')
    dfMaster = pd.read_csv("myProject/static/file/master.csv")
    picked = request.form.getlist('attributes')
    num = int(request.form.getlist('number')[0])

    p = Process()
    res = p.cus_seg(dfMaster,dfMerged,picked,num)
    cols = p.getCusCols(dfMerged)
    count = res[0]
    resDF = res[1]
    resDF.to_csv("myProject/static/file/cusRes1.csv",index = True)
    index = list(reversed(resDF.index.tolist()))
    clusters = list(resDF.columns)
    data = res[4]
    data.to_csv("myProject/static/file/cusRes2.csv",index = True)
    data = []
    max = 0.0
    for i in range(0, len(resDF)):
        for x,c in enumerate(clusters):
            data.append([x,len(resDF)-i-1,resDF.iloc[i][c]])
            if resDF.iloc[i][c]>max:
                max = resDF.iloc[i][c]
    print(data)
    return render_template('cus_seg_res.html',filename = "matched.csv",cols=cols,length = len(cols),k=res[3],values = count,clusters=clusters,attributes=picked,
    T = [resDF.to_html(classes='mystyle',formatters={'Name': lambda x: '<b>' + x + '</b>'})],index = index,data=data,maxValue = max);

@app.route("/downloadCus1",methods=['POST'])
def downloadCus1():
    df = pd.read_csv("myProject/static/file/cusRes1.csv")
    return GetData(df)()
@app.route("/downloadCus2",methods=['POST'])
def downloadCus2():
    df = pd.read_csv("myProject/static/file/cusRes2.csv")
    return GetData(df)()

@app.route("/seg/com_seg",methods=['GET','POST'])
def com_seg():
    dfMaster = pd.read_csv("myProject/static/file/master.csv")
    dfClient = pd.read_csv("myProject/static/file/upload/client.csv", sep='\t')
    dfMerged = pd.read_csv('myProject/static/file/matched.csv',dtype='str')
    p = Process()
    res = p.com_seg(dfMaster,dfMerged)

    count = res[1]
    cent = res[3]
    numOfC = len(count)
    cols = []
    for i in range(numOfC):
        cols.append("S "+str(i))
    clustroidsT = pd.DataFrame(cent.T,index=["transaction_sum","transaction_amount_sum"],columns=cols)
    resDF = clustroidsT
    resDF.to_csv("myProject/static/file/comRes1.csv",index = True)
    data = res[2]
    data.to_csv("myProject/static/file/comRes2.csv",index = True)
    clusters = []
    for i in range(len(count)):
        clusters.append('S'+str(i))
    return render_template('com_seg.html',k=res[0],values=count,clusters = clusters,filename = "matched.csv",
    T = [clustroidsT.to_html(classes='mystyle',formatters={'Name': lambda x: '<b>' + x + '</b>'})],index = clustroidsT.index.tolist());

@app.route("/downloadCom1",methods=['POST'])
def downloadCom1():
    df = pd.read_csv("myProject/static/file/comRes1.csv")
    return GetData(df)()

@app.route("/downloadCom2",methods=['POST'])
def downloadCom2():
    df = pd.read_csv("myProject/static/file/comRes2.csv")
    return GetData(df)()

@app.route("/seg/auto_seg",methods=['GET','POST'])
def auto_seg():
    return render_template('auto_seg.html')

@app.route("/downloadAuto",methods=['POST'])
def downloadAuto():
    df = pd.read_csv("myProject/static/file/autoRes.csv")
    return GetData(df)()

@app.route('/test')
def test():
    #index = [0,1,2]
    label = ["cont_id","prod_name","prod_family"]
    return render_template('test.html',label = label)

if __name__ == '__main__':
    app.run(debug=True)
