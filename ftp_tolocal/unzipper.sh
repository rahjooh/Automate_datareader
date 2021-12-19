#!/bin/bash
for i in ftproot_tolocal/*.zip;
do
  password=${i:29:4}
  password="isc1397$password"
  unzip -P $password -d data $i
 # rm $i
done
