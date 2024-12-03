import copy
from typing import Dict, List, Optional, Set
from collections import namedtuple
from itertools import product

PerformanceProfile = namedtuple('Profile', ['compute_time', 'input_time', 'output_time', 'minimum_vcpu'])
Edge = namedtuple('Edge', ['src', 'dst', 'weight'])

def get_topo(edge_list: List[Edge]) -> List[str]:
    stages: Set[str] = {edge.src for edge in edge_list} | {edge.dst for edge in edge_list}

    # get a topological order of the DAG
    topo: List[str] = []
    indeg = {stage: sum(edge.dst == stage for edge in edge_list) 
        for stage in stages}

    while len(topo) < len(stages):
        found = False
        for stage in stages:
            if stage not in topo and indeg[stage] == 0:
                topo.append(stage)
                found = True
                for edge in edge_list:
                    if edge.src == stage:
                        indeg[edge.dst] -= 1
        if found is False:
            raise Exception("Detected cycle in the DAG")
    return topo

class DittoPlacer:
    def __init__(self,
            node_resources: Dict[str, Dict[str, int]],
            dep: Dict[str, List[str]],
            stage_profile: Dict[str, PerformanceProfile],
        ) -> None:

        self.node_resources = node_resources
        self.dep = dep
        self.stage_profile = stage_profile

        self.stage_list = list(stage_profile.keys())

    def _get_critical_path(self, edge_list: List[Edge]) -> List[Edge]:
        if len(edge_list) == 0:
            return []
        
        # edge_list forms a DAG
        # weight of a path is the sum of weights of all edges and nodes in the path

        # get all nodes that occurs in the edge_list
        stages: Set[str] = {edge.src for edge in edge_list} | {edge.dst for edge in edge_list}

        # get a topological order of the DAG
        topo: List[str] =  get_topo(edge_list)

        # weight of an edge is output + input.

        # critical_path[stage_name] is the critical path from the stage to the end
        critical_path: Dict[str, List[Edge]] = {stage: [] for stage in stages}
        # critical_len records the length from this stage to end, excluding the stage input time.
        critical_len: Dict[str, float] = {stage: 0.0 for stage in stages}

        if len(topo) <= 1:
            return []
        
        last_in_topo = topo[len(topo)-1]
        critical_len[last_in_topo] = self.stage_profile[last_in_topo].compute_time
        critical_path[last_in_topo] = []
        for i in reversed(range(len(topo)-1)):
            cur = topo[i]
            # DP on cur, i+1 to len(topo)-1 has been known.
            # For every e.src == cur, e.dst has been known.
            critical_edge = max([e for e in edge_list if e.src == cur], key=lambda e: critical_len[e.dst] + e.weight,default=None)
            if critical_edge is None:
                critical_len[cur] = self.stage_profile[cur].compute_time
                critical_path[cur] = []
            else:
                critical_len[cur] = critical_len[critical_edge.dst] + critical_edge.weight \
                    + self.stage_profile[cur].compute_time
                critical_path[cur] = critical_path[critical_edge.dst] + [critical_edge]


        longest_path_endpoint = max(critical_len.items(), key=lambda x: x[1])[0]
        critical_path[longest_path_endpoint].reverse() # In reverse order.
        return critical_path[longest_path_endpoint]

    def _can_place(self, groups: List[Set[str]]) -> Optional[Dict[str, str]]:
        # search all possibilities for all stage groups to be put on all nodes.

        # get cpu needed for each stage group
        cpu_needed: List[int] = [sum(self.stage_profile[stage].minimum_vcpu for stage in group) for group in groups]

        # get all nodes
        nodes = self.node_resources.keys()

        for placement in product(nodes, repeat=len(groups)):
            if all( sum(cpu_needed[idx] for idx, loc in enumerate(placement) if loc == node) 
                        <= self.node_resources[node]['vcpu'] 
                for node in nodes):
                
                return {stage: loc 
                        for idx, loc in enumerate(placement)
                            for stage in groups[idx]}

        return None

    def run_placement_algorithm(self) -> Dict[str, str]:
        edge_list: List[Edge] = []
        for dst, deps in self.dep.items():
            for src in deps:
                edge_list.append(Edge(src, dst, self.stage_profile[src].output_time 
                                      + self.stage_profile[dst].input_time))

        # each stage is a single group
        groupings: List[Set[str]] = [set([stage]) for stage in self.dep.keys()]
        iter_times = 0
        
        ret_placement: Dict[str, str] = {}

        # check no binding case
        judge = self._can_place(groupings)
        
        if judge:
            ret_placement = judge
        else:
            raise Exception("Cannot place the DAG on the cluster even in the no binding case")

        while len(edge_list) > 0:
            iter_times += 1
            
            critical_path = self._get_critical_path(edge_list)
            
            if len(critical_path) == 0:
                break

            edge = max(critical_path, key=lambda e: e.weight)
            edge_list.remove(edge)
            
            src_group_idx = [idx for idx, group in enumerate(groupings) if edge.src in group][0]
            dst_group_idx = [idx for idx, group in enumerate(groupings) if edge.dst in group][0]
            
            if src_group_idx == dst_group_idx:
                continue

            new_groups = copy.deepcopy(groupings)

            # merge the two groups
            new_groups[src_group_idx].update(new_groups[dst_group_idx])
            del new_groups[dst_group_idx]

            placement = self._can_place(new_groups)
            if placement:
                ret_placement = placement
                groupings = new_groups

        return ret_placement