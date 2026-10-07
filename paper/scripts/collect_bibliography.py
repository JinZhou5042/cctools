#!/usr/bin/env python3
"""Retrieve publisher-deposited Crossref metadata; retain exact responses."""
import json,urllib.request,urllib.parse,time
from pathlib import Path
PAPER=Path(__file__).resolve().parents[1]
TITLES={
 'taskvine':'TaskVine: Managing In-Cluster Storage for High-Throughput Data Intensive Workflows',
 'hep':'Reshaping High-Energy Physics Applications for Near-Interactive Execution Using TaskVine',
 'grouping':'Liberating the Data Aware Scheduler to Achieve Locality in Layered Scientific Workflow Systems',
 'hoplite':'Hoplite: Efficient and Fault-Tolerant Collective Communication for Task-Based Distributed Systems',
 'pocket':'Pocket: Elastic Ephemeral Storage for Serverless Analytics',
 'exoflow':'ExoFlow: A Universal Workflow System for Exactly-Once DAGs',
 'wow':'WOW: Workflow-Aware Data Movement and Task Scheduling for Dynamic Scientific Workflows',
 'apollo':'Apollo: Scalable and Coordinated Scheduling for Cloud-Scale Computing',
 'sparrow':'Sparrow: Distributed, Low Latency Scheduling',
 'adaptive':'Adaptive Task-Oriented Resource Allocation for Large Dynamic Workflows on Opportunistic Resources',
 'legion':'Legion: Expressing Locality and Independence with Logical Regions',
 'parsl':'Parsl: Pervasive Parallel Programming in Python',
}
def main():
    out=PAPER/'research/bibliography-metadata';out.mkdir(exist_ok=True)
    for key,title in TITLES.items():
        path=out/f'{key}.json'
        if path.exists():continue
        url='https://api.crossref.org/works?'+urllib.parse.urlencode({'query.title':title,'rows':2})
        try:
            with urllib.request.urlopen(url,timeout=40) as response:data=json.load(response)
            path.write_text(json.dumps(data,indent=2)+'\n')
            print(key,[(v.get('title'),v.get('DOI')) for v in data['message']['items']],flush=True)
        except Exception as error:print(key,str(error),flush=True)
        time.sleep(.3)
if __name__=='__main__':main()
