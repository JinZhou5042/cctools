"""ATLAS diphoton ROOT reading, event selection and histogram reduction.

The graph preserves separate read, select and reduction tasks. The selection
matches the existing Hyy analysis, including its permissive eta predicate;
this experiment validates runtime equivalence, not a new physics selection.
"""
import hashlib
import json
import os
import platform
import time

MANIFEST = None
COLUMNS = ['photon_pt', 'photon_eta', 'photon_phi', 'photon_e',
           'photon_isTightID', 'photon_ptcone20']


def graph(workload, width, levels, size, cpu_ms):
    if MANIFEST is None:
        raise ValueError('a pinned ATLAS manifest is required')
    nodes = []
    leaves = []
    for index, item in enumerate(MANIFEST['files'][:width]):
        for start in range(0, item['entries'], size):
            tag = f'{index}:{start}'
            read = 'read:' + tag
            select = 'select:' + tag
            descriptor = dict(path=item['path'], start=start,
                              stop=min(start + size, item['entries']))
            nodes.append(dict(key=read, kind='read', size=descriptor,
                              cpu_ms=0, seed=index, parents=[]))
            nodes.append(dict(key=select, kind='select', size=0,
                              cpu_ms=0, seed=index, parents=[read]))
            leaves.append(select)
    stage = 0
    while len(leaves) > 1:
        parents = leaves
        leaves = []
        for i in range(0, len(parents), 8):
            key = f'reduce:{stage}:{i//8}'
            nodes.append(dict(key=key, kind='reduce', size=0, cpu_ms=0,
                              seed=0, parents=parents[i:i+8]))
            leaves.append(key)
        stage += 1
    return nodes, leaves


def paper_kernel(key, kind, size, cpu_ms, seed, *parents):
    import cloudpickle
    import numpy as np
    import awkward as ak
    import uproot

    entered = time.monotonic_ns()
    cpu = time.process_time_ns()
    records = {}
    values = []
    for parent in parents:
        if hashlib.sha256(parent['payload']).hexdigest() != parent['payload_sha256']:
            raise ValueError('corrupt ATLAS intermediate')
        records.update(parent['records'])
        values.append(cloudpickle.loads(parent['payload']))
    if kind == 'read':
        # A chunk contains real ROOT basket reads/decompression inside timing.
        with uproot.open(size['path'], num_workers=1) as source:
            data = source['analysis'].arrays(COLUMNS, entry_start=size['start'],
                entry_stop=size['stop'], library='ak')
        value = {'columns': data, 'entries': len(data)}
        answer = len(data)
    elif kind == 'select':
        data = values[0]['columns']
        cuts = [len(data)]
        # Preserve the reference's event order and selection exactly.
        mask = data.photon_isTightID[:, 0] & data.photon_isTightID[:, 1]
        data = data[mask]; cuts.append(len(data))
        data = data[(data.photon_pt[:, 0] > 50) & (data.photon_pt[:, 1] > 30)]
        cuts.append(len(data))
        iso = data.photon_ptcone20 / data.photon_pt
        data = data[(iso[:, 0] < .055) & (iso[:, 1] < .055)]
        cuts.append(len(data))
        eta = abs(data.photon_eta)
        data = data[((eta[:, 0] < 1.52) | (eta[:, 0] > 1.37)) &
                    ((eta[:, 1] < 1.52) | (eta[:, 1] > 1.37))]
        cuts.append(len(data))
        # Independent Cartesian calculation, compared to vector's p4 oracle.
        pt, eta, phi, energy = [data[name] for name in COLUMNS[:4]]
        px = pt[:, 0]*np.cos(phi[:, 0]) + pt[:, 1]*np.cos(phi[:, 1])
        py = pt[:, 0]*np.sin(phi[:, 0]) + pt[:, 1]*np.sin(phi[:, 1])
        pz = pt[:, 0]*np.sinh(eta[:, 0]) + pt[:, 1]*np.sinh(eta[:, 1])
        m2 = (energy[:, 0] + energy[:, 1])**2 - (px**2 + py**2 + pz**2)
        mass = np.sign(m2) * np.sqrt(abs(m2))
        good = mass != 0
        data, mass = data[good], mass[good]; cuts.append(len(data))
        good = (data.photon_pt[:, 0]/mass > .35) & (data.photon_pt[:, 1]/mass > .35)
        mass = mass[good]; cuts.append(len(mass))
        histogram, _ = np.histogram(ak.to_numpy(mass), bins=np.arange(100, 161))
        value = dict(histogram=histogram.tolist(), cutflow=cuts,
                     entries=cuts[0], selected=cuts[-1])
        answer = cuts[-1]
    elif kind == 'reduce':
        value = dict(histogram=np.sum([v['histogram'] for v in values], axis=0).tolist(),
                     cutflow=np.sum([v['cutflow'] for v in values], axis=0).tolist(),
                     entries=sum(v['entries'] for v in values),
                     selected=sum(v['selected'] for v in values))
        answer = value['selected']
    else:
        raise ValueError(kind)
    payload = cloudpickle.dumps(value)
    checksum = hashlib.sha256(payload).hexdigest()
    records[key] = dict(key=key, kind=kind, seed=seed, answer=answer,
        host=platform.node(), pid=os.getpid(), cpuset=sorted(os.sched_getaffinity(0)),
        start_ns=entered, finish_ns=time.monotonic_ns(),
        cpu_ns=time.process_time_ns()-cpu, input_bytes=sum(len(p['payload']) for p in parents),
        output_bytes=len(payload), local_io_bytes=0)
    return dict(payload=payload, payload_sha256=checksum, digest=checksum, records=records)


def validate(values, nodes):
    import cloudpickle
    import kernels
    records = kernels.validate(values, nodes)
    result = cloudpickle.loads(values[0]['payload'])
    file_indices = {n['seed'] for n in nodes if n['kind'] == 'read'}
    import numpy as np
    expected_files = [MANIFEST['files'][i]['oracle'] for i in sorted(file_indices)]
    expected = dict(histogram=np.sum([v['histogram'] for v in expected_files], axis=0).tolist(),
        cutflow=np.sum([v['cutflow'] for v in expected_files], axis=0).tolist(),
        entries=sum(v['entries'] for v in expected_files),
        selected=sum(v['selected'] for v in expected_files))
    if result != expected:
        raise ValueError(f'ATLAS independent oracle differs: got {result}; expected {expected}')
    return records
