import time
from typing import List, Dict
import sys
import json
import pickle

from faasit_runtime import function,workflow,create_handler
from faasit_runtime.workflow import WorkFlowBuilder
from faasit_runtime import FaasitRuntime

@function
async def threaded_output_function(frt: FaasitRuntime):
    suc = True
    input = frt.input()
    key_name = input.get('key_name')
    stage_name = input.get('stage_name')
    obj_to_send = input.get('obj_to_send')
    filename = None
    if stage_name == None:
        filename = key_name
    else:
        filename = f"{stage_name}/{key_name}"
    frt.storage.put(filename, pickle.dumps(obj_to_send))
    return frt.output({
        'suc':suc
    })

@function
async def threaded_input_function(frt: FaasitRuntime):
    suc = True
    input = frt.input()
    key_name = input.get('key_name',None)
    stage_name = input.get('stage_name',None)
    filename = None
    if stage_name == None:
        filename = key_name
    else:
        filename = f"{stage_name}/{key_name}"

    obj = frt.storage.get(filename)
    return frt.output({
        'suc':suc,
        'obj_to_recv': obj
    })


@function
async def mapper_handler(frt: FaasitRuntime):
    # input_address should be a single string.
    # Get its partition.
    # Split.
    # Map to tuple with counter one and aggregate.
    # Organize output stages and keys according to hash, and issue output.
    # Output is a dict.
    input = frt.input()

    num_reducers = input.get('num_reducers',0)
    assert(num_reducers > 0)
    stage: str = input['stage']
    task_id = int(stage.split('-')[-1])

    input_name = f'stage0-{task_id}-input'

    output_stages = [f'stage1-{tempi}' for tempi in range(num_reducers)]
    output_names = [f'stage1-{task_id}-{tempi}-input' for tempi in range(num_reducers)]

    output_dicts = []
    for i in range(num_reducers):
        output_dicts.append({})

    input_st = time.perf_counter()
    # TODO fetch data from redis
    res = await frt.call('threaded_input_function',{'key_name': input_name})
    obj:bytes = res['obj_to_recv']
    input_ed = time.perf_counter()
    input_time = input_ed - input_st
    input_str : str = obj.decode('utf-8')
    input_list : List[str] = input_str.split(" ")

    for word in input_list:
        hashval = hash(word) % num_reducers
        if word in output_dicts[hashval]:
            output_dicts[hashval][word] += 1
        else:
            output_dicts[hashval][word] = 1

    comed = time.perf_counter()
    compute_time = comed - input_ed
    output_thread_list = []
    retval_lists = []

    for i in range(num_reducers):
        t = frt.call('threaded_output_function',{
            'stage_name': output_stages[i],
            'key_name': output_names[i],
            'obj_to_send': output_dicts[i]
        })
        output_thread_list.append(t)

    for t in output_thread_list:
        retval_lists.append(await t)
    i = 0
    for retval_lst in retval_lists:
        suc = retval_lst['suc']
        if suc == False:
            print(f"Failed to output to reducer {i} in mapper {task_id}",file=sys.stderr)
            raise Exception(f"Failed to output to reducer {i} in mapper {task_id}")
        i += 1
    outed = time.perf_counter()
    output_time = outed - comed
    return frt.output({
        'input_time' : input_time,
        'compute_time' : compute_time,
        'output_time' : output_time,
        'total_time' : outed - input_st
    })
    

    
@function
async def reducer_handler(frt: FaasitRuntime):
    # Get its partition, a list of string.
    # Reduce to get the final results.
    input = frt.input()
    num_mappers = input.get('num_mappers',0)
    assert(num_mappers > 0)
    stage: str = input['stage']
    task_id = int(stage.split('-')[-1])
    input_stage_names = [f'stage1-{i}' for i in range(num_mappers)]
    input_key_names = [f'stage1-{task_id}-{i}-input' for i in range(num_mappers)]
    input_st = time.perf_counter()
    input_thread_list = []
    retval_lists = []
    for i in range(num_mappers):
        t = frt.call('threaded_input_function',{
            'stage_name': input_stage_names[i],
            'key_name': input_key_names[i]
        })
        input_thread_list.append(t)
    
    # Get a dict.
    # We can calculate while looping, but we cannot get input time in that way.
    # So we seperate input and calculation.
    dicts_list : List[Dict[str,int]] = []
    i = 0
    for t in input_thread_list:
        retval = await t
        suc = retval['suc']
        temp_dict_to_merge:bytes = retval['obj_to_recv']
        temp_dict_to_merge = pickle.loads(temp_dict_to_merge)
        if suc == False:
            print(f"Failed to get input from mapper {i} in reducer {task_id}",file=sys.stderr)
            raise Exception(f"Failed to get input from mapper {i} in reducer {task_id}")
        dicts_list.append(temp_dict_to_merge)
        i += 1
     
    input_ed = time.perf_counter()
    input_time = input_ed - input_st
    # num_mappers > 0.
    # Reduce.
    result_dict: Dict[str,int] = dicts_list[num_mappers-1]

    dicts_list.pop()
    pt = num_mappers - 2
    while pt >= 0:
        temp_dict:Dict[str,int] = dicts_list[pt]
        for word, val in temp_dict.items():
            if word in result_dict:
                result_dict[word] += val
            else:
                result_dict[word] = val
        dicts_list.pop()
        pt -= 1

    js_string = json.dumps(result_dict)
    comed = time.perf_counter()
    compute_time = comed - input_ed
    await frt.call('threaded_output_function',{'key_name': f'stage1-finalresult-{task_id}', 'obj_to_send': js_string})
    outed = time.perf_counter()
    output_time = outed - comed
    return frt.output({
        'input_time': input_time,
        'compute_time': compute_time,
        'output_time': output_time,
        'total_time': outed - input_st,
    })

@function
async def executor(frt: FaasitRuntime):
    mapper_stage : str = 'stage0'
    tasks = []
    for i in range(4):
        t = frt.call('mapper_handler',{
            'stage': f'{mapper_stage}-{i}',
            'num_reducers': 4
        })
        tasks.append(t)
    reducer_stage : str = 'stage1'
    for i in range(4):
        t = frt.call('reducer_handler',{
            'stage': f'{reducer_stage}-{i}',
            'num_mappers': 4
        })
        tasks.append(t)

    await frt.waitResults(tasks)
    return frt.output({
        'msg': 'ok'
    })

@workflow
def workflow(builder: WorkFlowBuilder):
    builder.func('mapper_handler').set_handler(mapper_handler)
    builder.func('reducer_handler').set_handler(reducer_handler)
    builder.func('threaded_output_function').set_handler(threaded_output_function)
    builder.func('threaded_input_function').set_handler(threaded_input_function)
    builder.executor().set_handler(executor)

    return builder.build()

handler = create_handler(workflow)