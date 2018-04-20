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
    partnr=document[5].split("               ")[1].strip() ; 
    description=document[5].split("               ")[2].strip();

    #line9 MRP type / Matl Type
    MRP=document[8].split()[3];
    MatlType=document[8].split()[6];

    #line11 Purchasing Group
    PurGrp=document[10].split()[2]; 

    #line12
    leadtime=document[11].split()[4];

    meta=[partnr,description, MRP, MatlType, PurGrp, leadtime]
    header=["Partnr", "Date","MRPElement","Rec/reqd Qty","AvailQty"]
    data=[]

    for line in document[22:len(document)-3] :
        cursor=line.split("|")
        #print("CURSOR :",  cursor[7], cursor[8], len(cursor[7].strip()), len(cursor[8].strip()))
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
        datum = [partnr, cursor[2].strip(), cursor[3].strip(), qty, avail]
        data.append(datum)

    md04={"meta":meta, "header": header, "data":data}
    return md04