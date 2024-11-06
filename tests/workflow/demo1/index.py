from faasit_runtime import function, workflow, create_handler
from faasit_runtime.runtime import FaasitRuntime
from faasit_runtime.workflow import Lambda,Workflow
from faasit_runtime.operators import forkjoin
import re
import time

@function
def count(frt: FaasitRuntime):
    time.sleep(1)
    _in = frt.input()
    words = _in["words"]
    
    counter = {}
    for word in words:
        if word in counter:
            counter[word] += 1
        else:
            counter[word] = 1
    return frt.output({
        "counter": list(counter.items())
    })

@function
def sort(frt: FaasitRuntime):
    time.sleep(1)
    _in = frt.input()
    counterArray = _in["counter"]

    counter = {}
    for arr in counterArray:
        if arr[0] not in counter:
            counter[arr[0]] = 0
        counter[arr[0]] += arr[1]

    reducedCounter = list(counter.items())
    reducedCounter.sort(key=lambda x: x[1], reverse=True)

    return frt.output({
        "counter": reducedCounter
    })

@function
def split(frt: FaasitRuntime):
    time.sleep(1)
    _in = frt.input()
    text: str = _in["text"]

    words = re.split(r'[\s,\.]', text)
    
    return frt.output({
        'message' : 'ok',
        'words': words
    })







@workflow
def wordcount(wf:Workflow):
    _in = wf.getEvent()
    text: str = _in.get('text')
    batchSize = _in.get('batchSize',10)
    
    # words = (await frt.call('split', {'text': text}))['words']
    words: Lambda = wf.call('split', {'text': text})['words']


    def work(words):
        result = wf.call('count', {'words': words})
        return result['counter']
    def join(counter):
        result = wf.call('sort', {'counter': counter})
        return result['counter']
    
    result = words.fork(3).map(work).join(join)

    return result

handler = create_handler(wordcount)