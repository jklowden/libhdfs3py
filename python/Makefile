.SUFFIXES: .c .so
INCLUDE = -I/usr/local/anaconda/include/python3.4m
CPPFLAGS = -g $(INCLUDE)
RPATH = $(dir $(PWD))build/src
LDFLAGS = -lhdfs3 -L$(RPATH) -Wl,-rpath -Wl,$(RPATH)
CC = c99 $(CPPFLAGS) $(CFLAGS) 
CXXFLAGS = -Wno-write-strings
CXX = c++ -x c++ -std=c++11 $(CPPFLAGS) $(CXXFLAGS) 

OUTPUT = hdfs3py.so
all: $(OUTPUT) t/hdfsBuilderSetNameNode

dist:
	cd t && python setup.py build

test: $(OUTPUT)
	PYTHONPATH=$(PWD)/t python t/numem.py

t/hdfsBuilderSetNameNode: t/hdfsBuilderSetNameNode.c
	$(CC) -o $@ $^ $(LDFLAGS)

ttest: t/hdfsBuilderSetNameNode

.c.so:
	$(CC) -o $@ $^ -fPIC -shared $(LDFLAGS)
