import json
import os
os.environ["FAASIT_PROVIDER"]="local-once"
from index import handler
output = handler({})
print(output)