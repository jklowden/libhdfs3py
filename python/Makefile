INCLUDE = -I/usr/local/anaconda/include/python3.4m
CC = c99 -g $(INCLUDE)

OUTPUT = hdfs3py.o
all: $(OUTPUT)

dist:
	cd t && python setup.py build

test: $(OUTPUT)
	PYTHONPATH=$(PWD)/t python t/numem.py

