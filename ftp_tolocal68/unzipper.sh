#!/bin/bash
for i in ftproot_tolocal/*.zip;
do
 password=${i:25:4}
 password="isc1397$password"
 unzip -P $password -d data $i
 rm $i
done
