# Configuration variables

TARGET = consus

export CROSSTEX_FLAGS:= --add-in --add-proceedings --titlecase=title --cite-by initials

# Rules that derive from configuration variables

.PHONY: all clean clobber

all: $(TARGET).pdf $(TARGET).ps
	@echo "SUCCESS: dvi, ps, pdf all up-to-date"

$(TARGET).dvi: $(TARGET).tex $(wildcard *.tex) $(wildcard */*.tex) $(wildcard */*/*.tex) assert-same.py latex.py Makefile
	python2 assert-same.py
	python2 latex.py --crosstex --index $(TARGET)

$(TARGET).ps: $(TARGET).dvi
	dvips $(TARGET).dvi

$(TARGET).pdf: $(TARGET).ps
	ps2pdf -dPDFSETTINGS=/prepress -dEmbedAllFonts=true $(TARGET).ps $(TARGET).pdf

-include $(TARGET).P

clean:
	rm -f $(TARGET).aux $(TARGET).bbl $(TARGET).log $(TARGET).stdout.log $(TARGET).pgf .$(TARGET).xtx.cache $(TARGET).P

clobber: clean
	rm -f $(TARGET).dvi $(TARGET).pdf $(TARGET).ps
