.SUFFIXES: .c .so
ANACONDA=/usr/local/anaconda
INCLUDE = -I$(ANACONDA)/include/python3.4m
CPPFLAGS = -g $(INCLUDE)
RPATH = $(dir $(PWD))build/src
PYRPATH = $(ANACONDA)/lib
LDFLAGS = -lhdfs3 -lpython3.4m \
	-L$(RPATH) -L$(PYRPATH) -Wl,-rpath -Wl,$(RPATH):$(PYRPATH)
CC = c99 $(CPPFLAGS) $(CFLAGS) 
CXXFLAGS = -Wno-write-strings
CXX = c++ -x c++ -std=c++11 $(CPPFLAGS) $(CXXFLAGS) 

OUTPUT = hdfs3py.so
all: $(OUTPUT) t/hdfsBuilderSetNameNode mnemo.o

dist:
	cd t && python setup.py build

test: $(OUTPUT)
	PYTHONPATH=$(PWD)/t python t/numem.py

t/hdfsBuilderSetNameNode: t/hdfsBuilderSetNameNode.c
	$(CC) -o $@ $^ $(LDFLAGS)

ttest: t/hdfsBuilderSetNameNode

mnemo.o: mnemo.h

.c.so:
	$(CC) -O0 -o $@ $^ -fPIC -shared $(LDFLAGS)
