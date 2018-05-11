#https://stackoverflow.com/questions/11869910/pandas-filter-rows-of-dataframe-with-operator-chaining
import pandas as pd

def fileRead(filename) :
    items=[]
    item=[]
    count=0
    toggle=False

    with open(filename,"r") as file:
    #ith open(filename,"r", encoding="UTF-8") as file:
    #with open(filename,"r", encoding="ISO-8859-1") as file:
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
            docNum=0
            pos=0
        try  :           

            if int(cursor[7][-1] == "-") :                      
                 qty=(-int(float((cursor[7][0:-1].strip()))))
            else:
                 qty=((int(float(cursor[7].strip()))))
        
        except Exception as e:
            qty=0
            #print(cursor[7], cursor[8], "Exception occured")  
            #print(e)
            
        try : 
            if int(cursor[8][-1] == "-") :      
                avail=(-int(float((cursor[8][0:-1].strip()))))                
            else:
                avail=((int(float(cursor[8].strip()))))                
                
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