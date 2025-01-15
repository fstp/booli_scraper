alias msh := connect_to_mongo

connect_to_mongo:
  mongosh "mongodb://root:root@localhost:27017"
