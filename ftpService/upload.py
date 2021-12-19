import requests
import sys
url = "http://farafekr.co/upload.php"
myFile = open(sys.argv[1],"rb")
r = requests.post(url,data={'s':'Upload'},files={'file':myFile})
print r.text
