"""
This file is reponsible for translating the profile.yaml file into kubernetes yaml files
and other deployment related stuffs.
"""

import copy
import yaml
import sys        
import os
from typing import Dict, List, Optional, Set
from collections import namedtuple
from itertools import product
import logging
from .serverless_utils import Address, DuplicatedPortChecker
import kubernetes as k8s

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

class DeploymentGenerator:
    def __init__(self, profile_path: str, random_placement: bool, *, local_placement: bool = False, knative: bool = False) -> None:
        # load the profile
        with open(profile_path, 'r') as f:
            self.profile = yaml.safe_load(f)

        self.knative = knative
        # self.node_resources: Dict[str, Dict[str, int]] = self.profile['node_resources']
        self.node_resources: Dict[str, Dict[str, int]] = self.get_node_info()
        self.nodes: Set[str] = set(self.node_resources.keys())
        self.stages: Set[str] = self.profile['stage_profiles'].keys()
        self.app_name = self.profile['app_name']
        self.external_ip = self.profile['external_ip']
        self.template_path = self.profile['template'] if knative is False else self.profile['knative_template']
        self.image_coldstart_latencies = self.profile['image_coldstart_latency']
        self.topo = get_topo([Edge(src, dst, 0) for dst, deps in self.profile['DAG'].items() for src in deps])

        self.dup_port_checker = DuplicatedPortChecker()
        for stage in self.stages:
            self.dup_port_checker.insert(self.profile['stage_profiles'][stage]['worker_external_port'])
            self.dup_port_checker.insert(self.profile['stage_profiles'][stage]['cache_server_external_port'])

        if knative:
            self.placement = {stage: '' for stage in self.stages} # knative is responsible for the placement
        elif random_placement:
            assert(local_placement is False)
            # place nodes evenly
            self.placement = {stage: list(self.nodes)[idx % len(self.nodes)] for idx, stage in enumerate(self.stages)}
        elif local_placement:
            # all stage are placed on 127.0.0.1
            self.placement = {stage: '127.0.0.1' for stage in self.stages}
        else:
            # run the placement algorithm

            self.placement = DittoPlacer(
                self.node_resources, 
                self.profile['DAG'], 
                {s: PerformanceProfile(input_time = self.profile['stage_profiles'][s]['input_time'],
                                    compute_time = self.profile['stage_profiles'][s]['compute_time'],
                                    output_time = self.profile['stage_profiles'][s]['output_time'],
                                    minimum_vcpu = self.profile['stage_profiles'][s]['request']['vcpu']) 
                for s in self.stages}).run_placement_algorithm()
            
        logging.info(f"Placement: {self.placement}")

    def getIngress(self) -> Dict[str, Address]:
        '''
        Return addresses that are available both in pod network and in node network.
        '''

        if self.knative:
            return {
                stage: Address(f"{self.app_name}-{stage}.default.10.0.0.233.sslip.io", # has to match kn_setup.sh
                    '',   # kantive is responsible for directing traffic to the worker port
                    None) # knative does not need cache server
                for stage in self.stages
            }
        else:   
            return {
                stage: Address(self.external_ip, 
                    self.profile['stage_profiles'][stage]['worker_external_port'],
                    self.profile['stage_profiles'][stage]['cache_server_external_port'])
                for stage in self.stages
            }

    def generate_kubernetes_yamls(self, output_folder: str) -> Dict[str, str]:
        
        if os.path.exists(output_folder) is False:
            os.mkdir(output_folder)

        files: Dict[str, str] = {
            stage: os.path.join(str(output_folder), f'{self.app_name}-{stage}.yaml') for stage in self.stages
        }
        
        # remove the old yaml files
        for file in files.values():
            if os.path.exists(file):
                os.remove(file)

        with open(self.template_path, 'r') as t:
            template_yaml = t.read()

            for stage, node in self.placement.items():
                with open(files[stage], 'a') as f:
                    f.write("# This file is auto-generated by spilot deployment.py\n")
                    f.write(self._replace_stage_varibles(template_yaml, stage))

        return files

    def _replace_stage_varibles(self, lines: str, stage: str) -> str:
        codeDir = self.profile['stage_profiles'][stage]['codeDir']
        return lines.replace('__app-name__', self.app_name)\
                    .replace('__stage-name__', stage)\
                    .replace('__node-name__', self.placement[stage])\
                    .replace('__image__', self.profile['stage_profiles'][stage]['image'])\
                    .replace('__command__', self.profile['stage_profiles'][stage]['command'])\
                    .replace('__args__', self.profile['stage_profiles'][stage]['args'])\
                    .replace('__worker-port__', str(self.profile['stage_profiles'][stage]['worker_port']))\
                    .replace('__cache-server-port__', str(self.profile['stage_profiles'][stage]['cache_server_port']))\
                    .replace('__worker-external-port__', str(self.profile['stage_profiles'][stage]['worker_external_port']))\
                    .replace('__cache-server-external-port__', str(self.profile['stage_profiles'][stage]['cache_server_external_port']))\
                    .replace('__parallelism__', str(self.profile['stage_profiles'][stage]['parallelism']))\
                    .replace('__external-ip__', self.external_ip)\
                    .replace('__host-path__', f'{os.getcwd()}/{codeDir}')\
                    .replace('__cwd__', os.getcwd())

    def get_worker_commandlines(self) -> Dict[str, str]:
        # fetch from the profile yaml file
        # combine command and args.

        ret = {}
        for stage in self.stages:
            ret[stage] = ' '.join(eval(self.profile['stage_profiles'][stage]['command'])) + ' ' \
                + ' '.join(eval(self.profile['stage_profiles'][stage]['args']))
            ret[stage] = self._replace_stage_varibles(ret[stage], stage)

        return ret


    def get_worker_start_point(self, timing_safe_guard = 0.5) -> Dict[str, float]:
        container_start_time = {s: 0.0 for s in self.stages}
        time_to_work = {s: 0.0 for s in self.stages} # the moment when the worker should start to work
        approximated_response_latency = {
            s: self.profile['stage_profiles'][s]['input_time']
               + self.profile['stage_profiles'][s]['compute_time']
               + self.profile['stage_profiles'][s]['output_time']
            for s in self.stages
        }

        for x in self.topo:
            time_to_work[x] = max([time_to_work[y] + approximated_response_latency[y] 
                            for y in self.profile['DAG'][x]], default = 0.0)

            image_coldstart_latency = self.image_coldstart_latencies[self.profile['stage_profiles'][x]['image']]
            container_start_time[x] = time_to_work[x] - image_coldstart_latency - timing_safe_guard

            if container_start_time[x] < 0:
                time_to_work[x] += -container_start_time[x]
                container_start_time[x] = 0

        logging.debug(f"container_start_time: {container_start_time}")
        logging.debug(f"time_to_work: {time_to_work}")
        logging.debug(f"approximated_response_latency: {approximated_response_latency}")

        return container_start_time

    def get_node_info(self) -> Dict[str, Dict[str, int]]:
        total_ret: Dict[str, Dict[str, int]] = {}
        k8s.config.load_kube_config()
        v1 = k8s.client.CoreV1Api()
        nodes = v1.list_node()

        for node in nodes.items:
            labels = node.metadata.labels
            is_control_plane = "node-role.kubernetes.io/master" in labels or "node-role.kubernetes.io/control-plane" in labels
            # skip the control plane in the node list
            # if is_control_plane:
            #     continue

            node_ret: Dict[str, int] = {}
            node_name = node.metadata.name
            node_ret['vcpu'] = int(node.status.capacity['cpu'])
            # TODO: decide the memory unit and its representation
            node_ret['memory'] = int(node.status.capacity['memory'].replace('Ki', '')) // 1024
            total_ret[node_name] = node_ret

            logging.debug(f"get information of node {node_name}: vcpu = {node_ret['vcpu']}, memory = {node_ret['memory']}")
        print(total_ret)
        return total_ret