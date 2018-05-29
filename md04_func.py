#https://stackoverflow.com/questions/11869910/pandas-filter-rows-of-dataframe-with-operator-chaining
import pandas as pd
import numpy as np
import datetime

category = {
    'ShipNt' :  "Receipt",
    'SchLne' :  "Receipt",    
    'Stock'  :  "Receipt",
    'PlOrd.'  :  "Receipt",
    'CStock' : "Reqmt",
    'Ordres' : "Reqmt",    
    'Order'  :  "Reqmt",
    'Deliv.' : "Reqmt",
    'PrdOrd' : "", #normally assigned together with cstock. no need to count
    'OrdRes' : "Reqmt",
    'TrRes.' : "Reqmt",
    'DepReq' : "Reqmt",
    'RelOrd' : "Reqmt",
    'Fr.del' : "Reqmt",
    'SafeSt' : "Reqmt",
    #'SafeSt' : "",
    'StLcSt' : "",
    'PurRqs' : "",
    'IndReq' : ""
}
header=["Partnr","SO_Date","SO_MRP", "SO_NR", "SO_POS","SO_QTY", "PO_DATE","PO_MRP","PO_NR", "PO_POS","PO_QTY" ]
total=[]


def fileRead(filename) :
    items=[]
    item=[]
    count=0
    toggle=False

    #with open(filename,"r") as file:
    #with open(filename,"r", encoding="UTF-8") as file:
    with open(filename,"r", encoding="ISO-8859-1") as file:
        document= file.readlines() 
    return document


def parsingDoc(document) :
    #line 6 for material # and material description
    partnr=document[5].split("             ")[1].strip() ; 
    description=document[5].split("             ")[2].strip();

    #line9 MRP type / Matl Type
    MRP=document[8].split()[3];
    MatlType=document[8].split()[6];

    #line11 Purchasing Group
    PurGrp=document[10].split()[2]; 

    #line12
    leadtime=document[11].split()[4];

    meta=[partnr,description, MRP, MatlType, PurGrp, leadtime]
    header=["Partnr", "Date","Type","DocNum","Pos","Change","Avail"]
    data=[]

    for line in document[22:-3] :
        cursor=line.split("|")
        #print("CURSOR :",  cursor[7], cursor[8], len(cursor[7].strip()), len(cursor[8].strip()))
        try :
            
            docNum=cursor[4].strip().split("/")[0]
            pos=cursor[4].strip().split("/")[1]
        
        except Exception as e:
            docNum=cursor[4].strip()
            pos=0
        try  :           

            if int(cursor[7][-1] == "-") :                      
                 qty=(-int(float((cursor[7][0:-1].strip().replace(",","")))))
            
            else:                
                qty=((int(float(cursor[7].strip().replace(",","")))))
        
        except Exception as e:
            qty=0
            print(cursor[7], cursor[8], "Exception occured")  
            print(e)
            
        try : 
            print(cursor[8].strip().replace(",",""))
            if int(cursor[8][-1] == "-") :      
                avail=(-int(float((cursor[8][0:-1].strip().replace(",","")))))
            else:
                avail=((int(float(cursor[8].strip().replace(",","")))))                
                
        except Exception as e:
            avail=0
            #print(cursor[7], cursor[8], "Exception occured")  
            #print(e)
        datum = [partnr, cursor[2].strip(), cursor[3].strip(), docNum, pos, qty, avail]
        print(datum)
        data.append(datum)
    
    
#     df=pd.DataFrame(data, columns=header) 
#     df.pos=pd.to_numeric(df.pos)
#     df.docNum=pd.to_numeric(df.docNum)

    md04={"meta":meta, "header": header, "data":data}
    return md04

#filter using chain method 
def mask(df, key, value):
    return df[df[key] == value]
pd.DataFrame.mask = mask





def kmatParser(df) : 
    MappedResult=[]
    idx=mask(df, "Type", "CStock").index
    CStockReqmt=df.iloc[idx]
    CStockReceipt=df.iloc[idx+1]

    if len(CStockReqmt) != len(CStockReceipt) : 
        print("Please check CStock as rows are not matched")
    
    for i,val in enumerate(CStockReceipt.iterrows()):
        MappedResult.append (CStockReqmt.iloc[i].tolist()[0:6] + (CStockReceipt.iloc[i].tolist()[1:6]))
    
    return MappedResult, df.drop(index=idx+1).drop(index=idx)   



def fehaParser(df) :
    ReceiptQueue=[]
    ReqmtQueue=[]
    MappedResult=[]
    flag=0
    Receipt=[]
    Reqmt=[]

    
    #KMAT PARSER
    MappedResult,df = kmatParser(df)    
    
    
    for index, line in df.iterrows() :    
        print(line)
        if line[6]==0  and line[2]== 'Stock' : continue
        if line[2] == 'Stock' : 
            line[5]=line[6]
        if category[line[2]] == 'Receipt' :        
            ReceiptQueue.append(line)
        if category[line[2]] == 'Reqmt' :
            ReqmtQueue.append(line)
        else :
            continue
            
    #If df only has kmat (CStock, then it return )
    if len(df)<=1 :        
        return MappedResult    
    
    # Else it goes through the below
    else : 
        try :   
            print("DF")
            print(df,"\n")
            print("MappedResult")
            print(MappedResult, "\n\n")
            print("ReceiptQueue")
            print(ReceiptQueue, "\n\n")
            print("ReqmtQueue")
            print(ReqmtQueue, "\n\n")
            Receipt = ReceiptQueue.pop(0)
            Reqmt = ReqmtQueue.pop(0)  

            while True :                 
                print (Receipt[5], Reqmt[5])
                if Receipt[5] + Reqmt[5] == 0 :                    
                    flag=0
                    MappedResult.append([Reqmt[0], Reqmt[1], Reqmt[2], Reqmt[3], Reqmt[4], Reqmt[5],  Receipt[1], Receipt[2], Receipt[3], Receipt[4], Receipt[5]])    
                    print("DF")
                    print(df,"\n")
                    print("ReceiptQueue")
                    print(ReceiptQueue, "\n")
                    print("ReqmtQueue")
                    print(ReqmtQueue, "\n\n")
                    
                    if len(ReceiptQueue) +  len(ReqmtQueue) <=0 : 
                        return MappedResult                        
                    
                    Receipt=[]; Receipt = ReceiptQueue.pop(0)
                    Reqmt=[]; Reqmt = ReqmtQueue.pop(0)  

                if Receipt[5] + Reqmt[5] > 0 :                           
                    flag=1
                    MappedResult.append([Reqmt[0], Reqmt[1], Reqmt[2], Reqmt[3], Reqmt[4], Reqmt[5],  Receipt[1], Receipt[2], Receipt[3], Receipt[4], -Reqmt[5]])    
                    Receipt[5] +=Reqmt[5]            
                    Reqmt = ReqmtQueue.pop(0)  


                if Receipt[5] + Reqmt[5] < 0 :                                
                    flag=-1
                    MappedResult.append([Reqmt[0], Reqmt[1], Reqmt[2], Reqmt[3], Reqmt[4], -Receipt[5],  Receipt[1], Receipt[2], Receipt[3], Receipt[4], Receipt[5]])
                    Reqmt[5] +=Receipt[5]
                    Receipt = ReceiptQueue.pop(0)

            #if no elment for popping.  
        except Exception as e:   

            if flag == -1:
                MappedResult.append([Reqmt[0], Reqmt[1], Reqmt[2], Reqmt[3], Reqmt[4], Reqmt[5],'','' ,'' ,'' ,'' ]) 
            elif flag == 1:
                MappedResult.append([Receipt[0], '','','','','',  Receipt[1], Receipt[2], Receipt[3], Receipt[4], Receipt[5]])          

            elif flag == 0:
                print("flag 0")
                print("Reqmt : ")
                print(Reqmt,"\n")
                
                print("Receipt : ")
                print(Receipt, "\n")
                
                
                if len(Reqmt) == 0 and len(Receipt)==0 :                     
                    print("both empty")
                elif len(Reqmt) == 0 and len(Receipt)!=0: 
                    #print("Reqmt empty")
                    MappedResult.append([Receipt[0], '','','','','',  Receipt[1], Receipt[2], Receipt[3], Receipt[4], Receipt[5]])          
                elif len(Reqmt) !=0 and len(Receipt) ==0 :                
                    #print("Receipt empty")
                    MappedResult.append([Reqmt[0], Reqmt[1], Reqmt[2], Reqmt[3], Reqmt[4], Reqmt[5],'','' ,'' ,'' ,'' ]) 

            #print("ReqmtQueue :", len(ReqmtQueue), "ReceiptQueue :", len(ReceiptQueue))
            for Reqmt in ReqmtQueue :
                MappedResult.append([Reqmt[0], Reqmt[1], Reqmt[2], Reqmt[3], Reqmt[4], Reqmt[5],'','' ,'' ,'' ,'' ]) 

            for Receipt in ReceiptQueue :
                MappedResult.append([Receipt[0], '','','','','',  Receipt[1], Receipt[2], Receipt[3], Receipt[4], Receipt[5]])          
            print ("!!!!!!!!!!!!!!!!!!!!!!!!")
            print (MappedResult)
            print ("!!!!!!!!!!!!!!!!!!!!!!!!")
            return MappedResult