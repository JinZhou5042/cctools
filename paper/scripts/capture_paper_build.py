#!/usr/bin/env python3
"""Bind the compiled PDF to its actual TeX, bibliography, and figure inputs."""
import hashlib
import json
from pathlib import Path
import re

PAPER = Path(__file__).resolve().parents[1]


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    files = {PAPER / 'references.bib'}
    pending = [PAPER / 'paper.tex']
    while pending:
        path = pending.pop()
        if path in files:
            continue
        files.add(path)
        text = path.read_text()
        for name in re.findall(r'\\input\{([^}]+)\}', text):
            pending.append(PAPER / (name if Path(name).suffix else name + '.tex'))
        for name in re.findall(r'\\includegraphics(?:\[[^]]*\])?\{([^}]+)\}', text):
            files.add(PAPER / 'figures' / name)
    (PAPER / 'build/manifest.json').write_text(json.dumps(dict(
        pdf_sha256=digest(PAPER / 'build/paper.pdf'),
        inputs={str(p.relative_to(PAPER)): digest(p) for p in sorted(files)},
        latex_log_sha256=digest(PAPER / 'build/latex-pass3.log')), indent=2) + '\n')


if __name__ == '__main__':
    main()
