.SUFFIXES: .c .so
INCLUDE = -I/usr/local/anaconda/include/python3.4m
CPPFLAGS = -g $(INCLUDE)
RPATH = $(dir $(PWD))dist/lib
LDFLAGS = -fPIC -shared -lhdfs3 -L$(RPATH) -Wl,-rpath -Wl,$(RPATH)
CC = c99 $(CPPFLAGS) $(CFLAGS) 

OUTPUT = hdfs3py.so
all: $(OUTPUT)

dist:
	cd t && python setup.py build

test: $(OUTPUT)
	PYTHONPATH=$(PWD)/t python t/numem.py

.c.so:
	$(CC) -o $@ $^ $(LDFLAGS)
