import numpy as np
import os
import pandas as pd
from myProject import app,db
from flask import render_template, redirect, request, url_for, flash,abort
from flask_login import login_user,login_required,logout_user
from myProject.forms import LoginForm, RegistrationForm
from myProject.extraction import GetData
from myProject.process import Process
from werkzeug.security import generate_password_hash, check_password_hash
#from werkzeug import secure_filename

@app.route('/', methods=['GET'])
def home():

    return render_template('home.html',length=0,mapping_res={},master_label=[],client_label=[])



@app.route('/upload', methods=['POST'])
def upload_file():
    file = request.files['file']
    #filename = secure_filename(file.filename)
    filename = "client.csv"
    file.save("myProject/static/file/"+filename)
    #flash("Upload Success!")
    return redirect(url_for('matchCol'))

@app.route('/matchCol',methods=['GET','POST'])
def matchCol():

    dfMaster = pd.read_csv("myProject/static/file/master.csv")
    dfClient = pd.read_csv("myProject/static/file/client.csv", sep='\t')
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
    p = Process(dfClient,dfMaster)

    mapping_dict = p.buildDict(master_label)
    mapping_res = p.match(client_label,master_label)
    return render_template('match.html',length=len(client_label),mapping_res=mapping_res,master_label=master_label,client_label=client_label)


@app.route('/merge',methods=['POST'])
def merge():
    #select = request.form.get('method_cipher')
    dfMaster = pd.read_csv("myProject/static/file/master.csv")
    dfClient = pd.read_csv("myProject/static/file/client.csv", sep='\t')
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

    return redirect((url_for('seg')))

@app.route("/downloadCSV",methods=['POST'])
def downloadCSV():
    dfMaster = pd.read_csv("myProject/static/file/master.csv")
    dfClient = pd.read_csv("myProject/static/file/client.csv", sep='\t')
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
    return render_template('seg.html');


@app.route("/seg/cus_seg",methods=['GET','POST'])
def cus_seg():
    return render_template('cus_seg.html');

@app.route("/downloadCus",methods=['POST'])
def downloadCus():
    df = pd.read_csv("myProject/static/file/cusRes.csv")
    return GetData(df)()

@app.route("/seg/com_seg",methods=['GET','POST'])
def com_seg():
    dfMaster = pd.read_csv("myProject/static/file/master.csv")
    dfClient = pd.read_csv("myProject/static/file/client.csv", sep='\t')
    dfMerged = pd.read_csv('myProject/static/file/matched.csv')
    p = Process(dfClient,dfMaster)
    res = p.com_seg(dfMaster,dfMerged)
    resDF = res[2]
    resDF.to_csv("myProject/static/file/comRes.csv",index = True)
    return render_template('com_seg.html',k=res[0],count=res[1]);
@app.route("/downloadCom",methods=['POST'])
def downloadCom():
    df = pd.read_csv("myProject/static/file/comRes.csv")
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
    return render_template('test.html')

if __name__ == '__main__':
    app.run(debug=True)
