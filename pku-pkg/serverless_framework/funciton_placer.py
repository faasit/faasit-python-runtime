"""
This file is reponsible for translating the profile.yaml file into kubernetes yaml files
and other deployment related stuffs.
"""

import yaml
import os
from typing import Dict, List, Optional, Set
from collections import namedtuple
import logging
import kubernetes as k8s

from .serverless_utils import Address, DuplicatedPortChecker, RuntimeType
from .placement import get_topo, DittoPlacer

PerformanceProfile = namedtuple('Profile', ['compute_time', 'input_time', 'output_time', 'minimum_vcpu'])
Edge = namedtuple('Edge', ['src', 'dst', 'weight'])

class DeploymentGenerator:
    def __init__(self, profile_path: str, random_placement: bool, *, local_placement: bool = False, knative: bool = False, runtime: RuntimeType,start_mode:str) -> None:
        # load the profile
        with open(profile_path, 'r') as f:
            self.profile = yaml.safe_load(f)

        self.knative = knative
        self.runtime = runtime
        self.start_mode = start_mode
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
                    f.write("# This file is auto-generated by sd-serverless funciton_placer.py\n")
                    f.write(self._replace_stage_varibles(template_yaml, stage))

        return files

    def _replace_stage_varibles(self, lines: str, stage: str) -> str:
        is_privileged = 'false'
        if self.start_mode == 'fast-start':
            is_privileged = 'true'
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
                    .replace('__privileged__', is_privileged)

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

    def is_usable_node(self, labels: Dict[str, str]) -> bool:
        is_control_plane = "node-role.kubernetes.io/master" in labels or "node-role.kubernetes.io/control-plane" in labels
        if self.runtime == RuntimeType.default:
            return (not is_control_plane) and (not "runtime" in labels)
        else:
            rt = str(self.runtime).split('.')[1]
            return (not is_control_plane) and ("runtime" in labels) and (labels["runtime"] == rt)


    def get_node_info(self) -> Dict[str, Dict[str, int]]:
        total_ret: Dict[str, Dict[str, int]] = {}
        k8s.config.load_kube_config()
        v1 = k8s.client.CoreV1Api()
        nodes = v1.list_node()

        for node in nodes.items:
            labels = node.metadata.labels
            
            # skip the control plane in the node list
            if self.is_usable_node(labels) is False and len(nodes.items) > 1:
                continue

            node_ret: Dict[str, int] = {}
            node_name = node.metadata.name
            node_ret['vcpu'] = int(node.status.capacity['cpu'])
            # TODO: decide the memory unit and its representation
            node_ret['memory'] = int(node.status.capacity['memory'].replace('Ki', '')) // 1024
            total_ret[node_name] = node_ret

            logging.debug(f"get information of node {node_name}: vcpu = {node_ret['vcpu']}, memory = {node_ret['memory']}")
        
        return total_ret