import os

def next(i , list ):
    if ( i >=( len(list) -1 ) ):
        return 0
    return 1


def check_order(list):
    if list == [] :
        return []
    for i in range(len(list)):
        if (next(i,list)):
            if (list[i] != list[i+1] -1 ):
                return ([list[0],list[i]+1] , i)
    return ([list[0],list[-1] +1 ],len(list) -1)


def create_list(list):
    if (list == [] ):
        return []
    answer = check_order(list)
    return answer[0] + create_list(list[answer[1]+1:])


def detect(input_path):
    files_names = os.listdir(input_path)
    return files_names

def rewrite (input_name , input_path , output_path , list,delimiter):
    bundry = create_bundry(list)
    bundryMinesOne = bundry[:-1]
    lastbundry = bundry[-1]
    output = open ( output_path +input_name , "w")
    with open( input_path +input_name ,'r') as file:
        for line in file:
            words = line.split(delimiter)
            if (len(words)>= (lastbundry[1] -1) ):
                for item in bundryMinesOne:
                    for p in range(item[0],item[1]):
                        output.write(str(words[p]) + delimiter)
                for item in range (lastbundry[0],lastbundry[1]):
                    if ( item != lastbundry[1] -1 ):
                        output.write(str(words[item]) + delimiter)
                    else:
                        output.write(str(words[item]) + "\n" )
    output.close()

def create_bundry(list):
    size = len(list) -  1
    bundry = []
    for i in range(0,size,2):
        bundry.append((list[i],list[i+1]))
    return bundry




def correct_saving_files(input_path , output_path,list,delimiter):
    names = detect(input_path)
    edited_list = create_list(list)
    for i in names:
        rewrite( i, input_path , output_path, edited_list , delimiter)
        print( i + ' Done!\n')


list = [0,1,2,3,8]
delimiter = '|'
output_path = '/home/hduser/ftp_tolocal/data2/'
input_path =  '/home/hduser/ftp_tolocal/data/'
correct_saving_files(input_path,output_path,list,delimiter)
