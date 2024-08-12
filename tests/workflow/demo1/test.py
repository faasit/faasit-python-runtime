# from dotenv import find_dotenv, load_dotenv
# load_dotenv(find_dotenv())
import os
os.environ["FAASIT_PROVIDER"]="local-once"
from index import handler
inputData = {"text":"Hello world this is a happy day","batchSize":3}
output = handler(inputData)
print(output)