import numpy as np
import pandas as pd
from difflib import SequenceMatcher
from sklearn.cluster import KMeans
from sklearn import metrics

class Process:
    def __init__(self,data1,data2):
        self.dfClient = data1
        self.dfMaster = data2

    def similarity(self,a, b):
        # use library difflib to get similarity
        return SequenceMatcher(None, a, b).ratio()

    def mapping(self,mapping_dict,all_columns,column):
        column = column.lower()
        # 1. find all possible matched columns in master table for the specific column in client table
        syn = {"postal_code":"zip",
                "code_postal":'zip',
                "first_name":"firstname",
                "last_name":"lastname"
        }

        candidates = set()
        res = []
        for key in mapping_dict:
            if column.find(key)!= -1:
                for w in mapping_dict[key]:
                    candidates.add(w)
            if column in syn.keys():
                if syn[column].find(key)!=-1:
                    for w in mapping_dict[key]:
                        candidates.add(w)
        # if we didn't find important substrings, then add all possible "other" columns
        if len(candidates)==0:
            for w in all_columns:
                res.append(w)
            return res

        # 2. rank the possible candidates
        res = list(candidates)
        res= sorted(candidates,key=lambda x: self.similarity(x,column),reverse=True)
        # if the similiarity between the most possible candidate and the column is 1, then we can match the column to it
        if self.similarity(res[0],column)==1:
            return [res[0]]
        return res

    def buildDict(self,labels):
        # split each label name on "_" to get all important substrings
        mapping_dict = {}
        for l in labels:
            substrs = l.split("_")
            for s in substrs:
                if not s in mapping_dict:
                    mapping_dict[s] = []
                mapping_dict[s].append(l)
        return mapping_dict


    def match(self,client_label,master_label):

        mapping_dict = self.buildDict(master_label)

        matchingRes = {}
        for c in client_label:
            if len(c)==0: continue
            matchingRes[c] = self.mapping(mapping_dict,master_label,c)


        return matchingRes

    def com_seg(self,dfMaster,dfMrged):
        data = []
        vals = dfMaster.values
        for row in vals:
            strs = row[0].split('|')
            data.append(strs)
        cols = dfMaster.columns.values[0].split('|')
        dfMaster = pd.DataFrame(data, columns=cols)
        master_label = list(dfMaster['master_label'])
        dateFeatures = {'cont_initial_contact_date','transaction_date','payment_date','delivery_date','date_of_record'}
        numFeatures = {'transaction_amount_tax', 'transaction_amount_net','prod_price_tax', 'prod_price_net'}
        otherFeatures = {'other01', 'other02', 'other03', 'other04', 'other05', 'other06', 'other07', 'other08', 'other09', 'other10'}
        cateFeatures = set(master_label)-numFeatures-otherFeatures

        dfMrged=dfMrged[~(dfMrged['payment_date'].isin(['TOM@TOMVITALEDESIGN.COM']))]
        dfMrged=dfMrged[~(dfMrged['cont_residency_zip'].isin(['satomi@yzdesignatrium.com']))]
        dfMrged=dfMrged[~(dfMrged['cont_id'].isin(['nan']))]
        curCols = list(dfMrged.columns.values)
        for col in curCols:
            if col in dateFeatures:
                dfMrged.loc[:,col]=pd.to_datetime(dfMrged.loc[:,col],errors='coerce')
                dfMrged[col] = dfMrged[col].dt.day_name()
            elif col in cateFeatures:
                dfMrged[col] = dfMrged[col].astype(str)
            elif col in numFeatures:
                dfMrged[col] = dfMrged[col].astype(float)
        filter1 = dfMrged.groupby('cont_id')['transaction_id'].nunique().to_frame()
        filter1 = filter1[(filter1['transaction_id']>=2)]
        filter2 = dfMrged.groupby('cont_id')['prod_price_net'].sum().to_frame()
        filter2 = filter2[(filter2['prod_price_net']>=50000)]
        data = pd.merge(filter1,filter2,on='cont_id',how='inner')
        data.columns = ['transaction_sum','transaction_amount_sum']
        SSE=self.elbow(data)
        k_best = -1
        alpha = 2
        while k_best==-1:
            for i in range(1,len(SSE)-1):
                if SSE[i-1]-SSE[i]>=alpha*(SSE[i]-SSE[i+1]):
                    k_best = i
            alpha = alpha*0.8
        model = KMeans(n_clusters=k_best+2,max_iter=1000, n_init=10)
        model.fit(data)
        y_pre = model.predict(data)
        data['cluster'] = y_pre
        count = []
        for i in range(k_best+2):
            count.append(len(np.where(y_pre == i)[0]))
        return [k_best+2,count,data]

    def elbow(self,data):
        SSE=[]
        k_vals=[2,3,4,5,6,7,8,9,10,11]
        for k in k_vals:
            model=KMeans(n_clusters=k,max_iter=500, n_init=10)
            model.fit(data)
            SSE.append(model.inertia_)
        return SSE
