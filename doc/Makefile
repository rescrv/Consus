.PHONY: all clean clobber data

# Configuration variables

TARGET = consus

export CROSSTEX_FLAGS:= --add-in --add-proceedings --titlecase=title

# Rules that derive from configuration variables

all: $(TARGET).pdf $(TARGET).ps
	@echo "SUCCESS: dvi, ps, pdf all up-to-date"

$(TARGET).dvi: $(TARGET).tex $(TARGET).xtx latex.py Makefile
	python2 latex.py $(TARGET)

$(TARGET).ps: $(TARGET).dvi
	dvips $(TARGET).dvi

$(TARGET).pdf: $(TARGET).ps
	ps2pdf -dPDFSETTINGS=/prepress -dEmbedAllFonts=true $(TARGET).ps $(TARGET).pdf

-include $(TARGET).P

clean:
	rm -f $(TARGET).aux $(TARGET).bbl $(TARGET).log $(TARGET).stdout.log $(TARGET).pgf .$(TARGET).xtx.cache $(TARGET).P

clobber: clean
	rm -f $(TARGET).dvi $(TARGET).pdf $(TARGET).ps
