alias msh := connect_to_mongo
alias lab := start_jupyter

connect_to_mongo:
  mongosh "mongodb://root:root@localhost:27017"

start_jupyter:
  . venv/bin/activate && jupyter lab --no-browser --port=8888 --notebook-dir=./notebooks
