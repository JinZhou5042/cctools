#!/usr/bin/env python3
"""Audit measured research evidence, the final implementation, and its PDF."""
import hashlib
import ast
import json
from pathlib import Path
import re
import subprocess
import sys

PAPER = Path(__file__).resolve().parents[1]
ROOT = PAPER.parent


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    checks = []
    def check(name, passed, detail=''):
        checks.append(dict(check=name, passed=bool(passed), detail=detail))
    done = subprocess.run([sys.executable, str(PAPER / 'scripts/summarize_upgrade.py'), '--check-only'], capture_output=True, text=True)
    check('complete upgraded campaigns', done.returncode == 0, done.stdout.strip())
    evidence = json.loads((PAPER / 'results/upgrade-audit.json').read_text())
    check('345 accepted trials', evidence['status'] == 'PASS' and evidence['accepted_trials'] == 345)
    for relative in ['figures/upgrade-provenance.json', 'results/upgrade-traces.json']:
        path = PAPER / relative
        if not path.exists():
            check(relative, False, 'missing'); continue
        data = json.loads(path.read_text())
        check(relative, all((PAPER / key).exists() and sha(PAPER / key) == value
                           for key, value in data['inputs'].items()))
        if 'outputs' in data:
            check(relative + ' outputs', all(sha(PAPER / key) == value for key, value in data['outputs'].items()))
    regression = json.loads((PAPER / 'results/upgrade-regression-final-v3.json').read_text())
    check('current regression', regression['status'] == 'PASS' and regression['passed_count'] == regression['test_count'] == 25)
    check('regression fingerprint manifest retained', bool(regression.get('fingerprints')))
    mechanism_path = PAPER / 'results/upgrade-mechanism-final/summary.json'
    check('current eager/deferred and generic serverless', mechanism_path.exists() and json.loads(mechanism_path.read_text())['status'] == 'PASS')
    oracle = json.loads((PAPER / 'results/atlas-inputs-v1/manifest.json').read_text())
    check('full independent scientific oracle', oracle['status'] == 'PASS' and
          len(oracle['files']) == 16 and oracle['total_bytes'] == 9861498743 and
          oracle['oracle']['entries'] == 36564144 and oracle['oracle']['selected'] == 553456)
    app = json.loads((PAPER / 'results/upgrade-application-v4/summary.json').read_text())
    check('application equals independent oracle', all(
        row['research']['scientific_output'] == oracle['oracle'] for row in app['runs']))
    current_path = PAPER / 'results/upgrade-current-application-amd/summary.json'
    current = json.loads(current_path.read_text()) if current_path.exists() else {}
    check('measured-build full application replay', current.get('status') == 'PASS' and
          current.get('passed') == 3 and all(r['research']['scientific_output'] == oracle['oracle']
          for r in current.get('runs', [])))
    saved = PAPER / 'provenance/measured-upgrade-v1'
    alternate_path = PAPER / 'results/upgrade-current-application-intel/summary.json'
    alternate = json.loads(alternate_path.read_text()) if alternate_path.exists() else {}
    independent_path = PAPER / 'results/upgrade-intel-oracle-v2.json'
    independent = json.loads(independent_path.read_text())
    platform = json.loads((PAPER / 'results/atlas-inputs-intel-v1.json').read_text())
    rejected = json.loads((PAPER / 'results/upgrade-current-application-smoke/summary.json').read_text())['runs'][0]
    observed = ast.literal_eval(rejected['error'].split('got ', 1)[1].split('; expected ', 1)[0])
    check('independent platform reference explains rejected output', rejected['status'] == 'FAIL' and
          independent['status'] == 'PASS' and observed == independent['oracle'] == platform['oracle'] and
          independent['reference_sha256'] == oracle['oracle_source_sha256'] and
          platform['numerical_platform']['oracle_report_sha256'] == sha(independent_path))
    check('second-platform measured-build application replay', alternate.get('status') == 'PASS' and
          alternate.get('passed') == 3 and all(r['research']['scientific_output'] == independent['oracle']
          for r in alternate.get('runs', [])))
    manifest = json.loads((saved / 'manifest.json').read_text())
    check('exact measured binaries retained', all(sha(saved / key) == value for key, value in manifest['files'].items()) and
          sha(saved / 'source.patch') == manifest['source_patch_sha256'])
    review = json.loads((PAPER / 'provenance/upgrade-source-audit.json').read_text())
    check('review patch matches current implementation', all(sha(ROOT / item['path']) == item['after']
          for item in review['files'] if item['path'] != 'taskvine/src/tools/datavine_workflow.c') and
          sha(PAPER / 'provenance/upgrade-implementation.patch') == review['patch_sha256'])
    decoupling = json.loads((PAPER / 'results/controller-decoupling-20260906.json').read_text())
    check('current controller decoupling evidence', decoupling['status'] == 'PASS_WITH_LIMITS' and
          decoupling['implementation']['binary_sha256'] == sha(ROOT / 'taskvine/src/tools/datavine_workflow') and
          decoupling['implementation']['workflow_source_sha256'] == sha(ROOT / 'taskvine/src/tools/datavine_workflow.c') and
          decoupling['implementation']['rpc_source_sha256'] == sha(ROOT / 'taskvine/src/datavine/vine_datavine_rpc.c') and
          (PAPER / 'results/controller-decoupling-table.tex').exists())
    raw_decoupling = list((PAPER / 'results/controller-decoupling-raw').glob('*.json'))
    check('controller decoupling raw runs retained', len(raw_decoupling) == 13 and all(
        sha(PAPER / 'results' / key) == value
        for key, value in decoupling.get('raw_sha256', {}).items()))
    bottleneck_path = PAPER / 'results/bottleneck-scale/summary.json'
    bottleneck = json.loads(bottleneck_path.read_text()) if bottleneck_path.exists() else {}
    raw_bottleneck = list((PAPER / 'results/bottleneck-scale/raw').glob('tasks-*.json'))
    check('background bottleneck evidence', bottleneck.get('status') == 'PASS' and
          len(raw_bottleneck) == 6 and
          (PAPER / 'figures/bottleneck-motivation.pdf').exists() and
          all(sha(ROOT / key) == value
              for key, value in bottleneck.get('inputs_sha256', {}).items()))
    # Legacy failure evidence is cited with its own version; never require an
    # old measurement to pretend it ran the subsequently repaired binary.
    churn = json.loads((PAPER / 'results/worker-churn-v3/summary.json').read_text())
    check('retained worker-loss evidence', churn['status'] == 'PASS' and
          churn['correctness']['journal']['unique_completed_tasks'] == 1024 and churn['correctness']['all_sinks_match'])
    before = json.loads((PAPER / 'provenance/starting-dirty-files.json').read_text())
    allowed = {'DATAVINE_PRODUCTION.md', 'acceptance/README.md', 'acceptance/matrix.md',
        'acceptance/scripts/benchmark_worker_churn.py', 'acceptance/scripts/run_regression.sh',
        'taskvine/src/worker/vine_process.h', 'taskvine/src/worker/vine_worker.c',
        'taskvine/src/manager/vine_manager.c', 'taskvine/src/manager/vine_function_call.c',
        'taskvine/src/manager/vine_worker_pool.c',
        'taskvine/src/manager/vine_worker_pool.h', 'taskvine/src/tools/datavine_workflow.c',
        'taskvine/src/datavine/vine_datavine_rpc.c', 'taskvine/test/datavine_workflow_service.py',
        'acceptance/scripts/benchmark_controller_rpc.py'}
    changed = {name for name, digest in before.items() if not (ROOT / name).exists() or sha(ROOT / name) != digest}
    check('unrelated inherited work preserved', not changed - allowed, str(sorted(changed - allowed)))
    tracked = set(subprocess.check_output(['git', '-C', str(ROOT), 'diff', '--name-only'], text=True).splitlines())
    check('tracked implementation scope', not tracked - set(before) - allowed,
          str(sorted(tracked - set(before) - allowed)))
    pdf = PAPER / 'build/paper.pdf'
    build = PAPER / 'build/manifest.json'
    check('compiled PDF with input manifest', pdf.exists() and build.exists())
    if pdf.exists() and build.exists():
        sources = json.loads(build.read_text())
        check('PDF matches manuscript and figures', sha(pdf) == sources['pdf_sha256'] and all(
            sha(PAPER / key) == value for key, value in sources['inputs'].items()))
        text = subprocess.check_output(['pdftotext', '-layout', str(pdf), '-'], text=True)
        pages = text.split('\f'); body = len(pages) - 1
        for index, page in enumerate(pages):
            match = re.search(r'R\s*E\s*F\s*E\s*R\s*E\s*N\s*C\s*E\s*S', page)
            if match:
                body = index + int(bool(page[:match.start()].strip())); break
        check('body page limit', body <= 10, f'{body} pages including any page shared with references')
        check('anonymous PDF', not re.search(r'jzhou24|/users/|/groups/|condorfe|crc\.nd\.edu', text))
        check('AI disclosure', 'ACKNOWLEDGMENTS' in text and 'OpenAI Codex' in text)
        log = (PAPER / 'build/latex-pass3.log').read_text()
        check('references and layout', not re.search(r'undefined|Overfull|LaTeX Error', log))
    bib = (PAPER / 'references.bib').read_text()
    check('bibliography source links and authors', 'and others' not in bib and all(
        'author' in entry and ('url=' in entry or 'url =' in entry or 'doi=' in entry or 'doi =' in entry)
        for entry in bib.split('@')[1:]))
    report = dict(status='PASS' if all(c['passed'] for c in checks) else 'OPEN', checks=checks,
        readiness='Evidence, implementation validation, and document format only. Does not certify conference acceptance, exclusive nodes, prepared-node scheduling, or production collaboration readiness.')
    (PAPER / 'results/upgrade-artifact-audit.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))
    return 0 if report['status'] == 'PASS' else 1


if __name__ == '__main__':
    raise SystemExit(main())
