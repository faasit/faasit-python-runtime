from faasit_runtime.runtime import FaasitRuntime
from faasit_runtime.workflow import WorkFlowBuilder
from faasit_runtime import function, durable, create_handler, workflow

@function
async def workeradd(frt: FaasitRuntime):
    input = frt.input()
    lhs = input['lhs']
    rhs = input['rhs']
    return frt.output({
        "res": lhs+ rhs
    })

@durable
async def durChain2(frt: FaasitRuntime):
    input = frt.input()
    lhs = input.get('lhs')
    rhs = input.get('rhs')
    r1 = await frt.call('workeradd', {"lhs": lhs, "rhs": rhs})
    return frt.output(r1)

@durable
async def durChain(frt: FaasitRuntime):
    r1 = await frt.call('durChain2', {"lhs": 1, "rhs": 2})
    r2 = await frt.call('durChain2', {"lhs": r1['res'], "rhs": 3})
    r3 = await frt.call('durChain2', {"lhs": r2['res'], "rhs": 4})
    return frt.output(r3)

@durable
async def durRecursive(frt: FaasitRuntime):
    r1 = await frt.call('durChain', {})
    r2 = await frt.call('durChain', {})
    r3 = await frt.call('workeradd', {"lhs": r1['res'], "rhs": r2['res']})
    return frt.output(r3)

@function
async def exetutor(frt: FaasitRuntime):
    r = await frt.call('durRecursive',{})
    return r

@workflow
def workflow(builder: WorkFlowBuilder):
    builder.func('workeradd').set_handler(workeradd)
    builder.func('durChain').set_handler(durChain)
    builder.func('durChain2').set_handler(durChain2)
    builder.func('durRecursive').set_handler(durRecursive)
    builder.executor().set_handler(exetutor)
    return builder.build()

handler = create_handler(workflow)