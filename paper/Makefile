PYTHON ?= python
TEXENV = TEXINPUTS=vendor:$(CURDIR):$(CURDIR)/sections: BSTINPUTS=vendor:
.PHONY: all figures paper audit audit-legacy
all: paper
figures:
	$(PYTHON) scripts/plot_results.py
	$(PYTHON) scripts/plot_supplemental.py
	$(PYTHON) scripts/plot_preloaded.py
	$(PYTHON) scripts/analyze_upgrade_traces.py
	$(PYTHON) scripts/summarize_upgrade.py
	$(PYTHON) scripts/plot_bottleneck.py
paper: figures
	mkdir -p build
	$(TEXENV) pdflatex -halt-on-error -interaction=nonstopmode -output-directory=build paper.tex > build/latex-pass1.log
	$(TEXENV) bibtex build/paper > build/bibtex.log
	$(TEXENV) pdflatex -halt-on-error -interaction=nonstopmode -output-directory=build paper.tex > build/latex-pass2.log
	$(TEXENV) pdflatex -halt-on-error -interaction=nonstopmode -output-directory=build paper.tex > build/latex-pass3.log
	$(PYTHON) scripts/capture_paper_build.py
audit:
	$(PYTHON) scripts/audit_upgrade_artifact.py
audit-legacy:
	$(PYTHON) scripts/audit_artifact.py
