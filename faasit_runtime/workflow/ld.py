from typing import Any, TYPE_CHECKING
from importlib import import_module
if TYPE_CHECKING:
    from .dag import DataNode,ControlNode
    from .workflow import Workflow

class Lambda:
    def __init__(self, value: Any | None = None) -> None:
        self.value = value
        self._dataNode: "DataNode" = None

    def getDataNode(self):
        return self._dataNode
    
    def setDataNode(self, node:"DataNode"):
        self._dataNode = node

    def _generate_dag(self, fn, list_lambda: list["Lambda"]) -> "Lambda":
        ControlNode:"ControlNode" = import_module('faasit_runtime.workflow.dag').ControlNode
        DataNode:"DataNode" = import_module('faasit_runtime.workflow.dag').DataNode
        Workflow:"Workflow" = import_module('faasit_runtime.workflow.workflow').Workflow
        invoke_fn = Workflow.funcHelper(fn)
        fn_ctl_node = ControlNode(invoke_fn)
        for index,ld in enumerate(list_lambda):
            param_node = DataNode(ld) if ld.getDataNode() == None else ld.getDataNode()
            param_node.add_succ_control_node(fn_ctl_node)
            fn_ctl_node.add_pre_data_node(param_node)
            fn_ctl_node.defParams(ld,index)

        r = Lambda()
        result_node = DataNode(r)
        fn_ctl_node.set_data_node(result_node)
        result_node.set_pre_control_node(fn_ctl_node)
        return r

    def __add__(self, other: Any) -> "Lambda":
        if not isinstance(other, Lambda):
            other = Lambda(other)
        return self._generate_dag(lambda x, y: x + y, [self, other])
    
    def __getitem__(self, key: str) -> "Lambda":
        if not isinstance(key, Lambda):
            key = Lambda(key)
        return self._generate_dag(lambda dir, key: dir[key], [self,key])
    
    def __call__(self, *args: Any, **kwds: Any) -> "Lambda":
        
        pass

    def __iter__(self) -> "Lambda":
        return self
        return self._generate_dag(lambda x: iter(x), [self])
    
    def __str__(self) -> str:
        return f"{super().__str__()}::{self.value}"