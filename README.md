# cs505_project


## kick off orientdb

open both port 2424 and 2480 from openstack dashboard and execute following in cmd to kick off docer instance of orientdb
`sudo docker run -d --name orientdb -p 2424:2424 -p 2480:2480 -e ORIENTDB_ROOT_PASSWORD=password orientdb:3.0.0`

## Build and execute the jar file using follwing command
bash build.sh


## API queries for testing
- mf_1# http://maal281.cs.uky.edu:8082/api/getteam
- mf_2# http://maal281.cs.uky.edu:8082/api/reset
- rtr_1# http://maal281.cs.uky.edu:8082/api/zipalertlist
- rtr_2# http://maal281.cs.uky.edu:8082/api/alertlist
- ct_1# http://maal281.cs.uky.edu:8082/api/getconfirmedcontacts/{mrn}
- ct_2# http://maal281.cs.uky.edu:8082/api/getpossiblecontacts/{mrn} 
- of_1# http://maal281.cs.uky.edu:8082/api/getpatientstatus/{hospital_id} 
- of_2# http://maal281.cs.uky.edu:8082/api/getpatientstatus/{hospital_id} 

