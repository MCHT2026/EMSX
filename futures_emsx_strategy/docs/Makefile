.PHONY: all clean

# Prefer lualatex (better font handling) but fall back to pdflatex.
ENGINE := $(shell command -v lualatex 2>/dev/null || command -v pdflatex)

all: contracts.pdf

contracts.pdf: contracts.tex
	$(ENGINE) -interaction=nonstopmode contracts.tex
	$(ENGINE) -interaction=nonstopmode contracts.tex

clean:
	rm -f contracts.aux contracts.log contracts.out contracts.toc contracts.pdf
