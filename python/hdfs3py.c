#include <Python.h>
#include <memoryobject.h>
#include <stdbool.h>

#include "../src/client/hdfs.h"

static const char bld_capsule_name[] = "hdfs3py.hdfsBuilder";
static const char file_capsule_name[] = "hdfs3py.hdfsFile";
static const char fileinfo_capsule_name[] = "hdfs3py.hdfsFileInfo";
static const char fs_capsule_name[] = "hdfs3py.hdfsFS";

static PyObject *
hdfs_err() {
  PyObject *type = PyErr_NewException("hdfs3py.error", NULL, NULL);
  const char *msg = hdfsGetLastError();
  PyErr_SetString(type, msg);
  return NULL;
}  

static const char getLastError_doc[] =
  "Return error information of last failed operation";
static PyObject *
getLastError() {
  return PyUnicode_FromString(hdfsGetLastError());
}

static int 
extract_fs( PyObject * fs_capsule, void *pout ) {
  hdfsFS *pfs = (hdfsFS*)pout;
  if( (pout = PyCapsule_GetPointer(fs_capsule, fs_capsule_name)) == NULL ) {
    hdfs_err();
    return 0;
  }
  *pfs = *(hdfsFS*)pout;
  return 1;
}

static int 
extract_file( PyObject * file_capsule, void *pout ) {
  hdfsFile *pfile = (hdfsFile*)pout;
  if( (pout = PyCapsule_GetPointer(file_capsule, file_capsule_name)) == NULL ) {
    hdfs_err();
    return 0;
  }
  *pfile = *(hdfsFile*)pout;
  return 1;
}

static const char fileIsOpenForRead_doc[] = 
  "Determine if a file is open for read";
static PyObject *
fileIsOpenForRead(PyObject * args) {
  hdfsFile file;
  if( !PyArg_ParseTuple(args, "O&", extract_file, &file) ) {
    return NULL;
  }
  if( hdfsFileIsOpenForRead(file) ) 
    Py_RETURN_TRUE;
  Py_RETURN_FALSE;
}

static const char fileIsOpenForWrite_doc[] = 
  "Determine if a file is open for write";
static PyObject *
fileIsOpenForWrite(PyObject * args) {
  hdfsFile file;
  if( !PyArg_ParseTuple(args, "O&", extract_file, &file) ) {
    return NULL;
  }
  if( hdfsFileIsOpenForWrite(file) ) 
    Py_RETURN_TRUE;
  Py_RETURN_FALSE;
}

static const char connect_doc[] = 
  "Connect to an hdfs file system, as current or other user, "
  "with or without a new instance";
static PyObject *
connect(PyObject * args, PyObject *keywords) {
  static char *okwords[] = { "user", "new_instance" };
  const char *nn, *user = NULL;
  tPort port;
  bool is_new_instance = false;
  hdfsFS fs;
  PyObject* fs_capsule;

  if( !PyArg_ParseTupleAndKeywords(args, keywords, "sh$zp", okwords, 
				   &nn, &port, &user, &is_new_instance) ) {
    return NULL;
  }

  if( is_new_instance ) {
    if( user ) {
      if( (fs = hdfsConnectAsUserNewInstance(nn, port, user)) == NULL ) {
	return hdfs_err();
      }
    }
    if( (fs = hdfsConnectNewInstance(nn, port)) == NULL ) {
      return hdfs_err();
    }
  } else {
    if( user ) {
      if( (fs = hdfsConnectAsUser(nn, port, user)) == NULL ) {
	return hdfs_err();
      }
    }
    if( (fs = hdfsConnect(nn, port)) == NULL ) {
      return hdfs_err();
    }
  }

  if( (fs_capsule = PyCapsule_New(fs, fs_capsule_name, NULL)) == NULL ) {
    return NULL;
  }
  return fs_capsule;  
}

static const char disconnect_doc[] = 
  "Disconnect from the hdfs file system";
static PyObject *
disconnect(PyObject * args) {
  hdfsFS fs;
  if( !PyArg_ParseTuple(args, "O&", extract_fs, &fs) ) {
    return NULL;
  }
  if( 0 != hdfsDisconnect(fs) ) {
    return hdfs_err();
  }
  Py_RETURN_TRUE;
}

static int 
extract_bld( PyObject * bld_capsule, void *pout ) {
  struct hdfsBuilder **pbld = (struct hdfsBuilder **)pout;
  if( (pout = PyCapsule_GetPointer(bld_capsule, bld_capsule_name)) == NULL ) {
    hdfs_err();
    return 0;
  }
  *pbld = *(struct hdfsBuilder **)pout;
  return 1;
}

static const char builderConnect_doc[] = 
  "Connect to HDFS using the parameters defined by the builder";
static PyObject *
builderConnect(PyObject * args) {
  hdfsFS fs;
  PyObject * fs_capsule;
  struct hdfsBuilder * bld;
  if( !PyArg_ParseTuple(args, "O&", extract_bld, &bld) ) {
    return NULL;
  }

  if( (fs = hdfsBuilderConnect(bld)) == NULL ) {
    return hdfs_err();
  }
  if( (fs_capsule = PyCapsule_New(fs, fs_capsule_name, NULL)) == NULL ) {
    return NULL;
  }
  return fs_capsule;  
}

static const char newBuilder_doc[] = "Create an HDFS builder";
PyObject *
newBuilder(void) {
  PyObject* bld_capsule;
  struct hdfsBuilder * bld;
  if( (bld = hdfsNewBuilder()) == NULL ) {
    return hdfs_err();
  }
  if( (bld_capsule = PyCapsule_New(bld, bld_capsule_name, NULL)) == NULL ) {
    return NULL;
  }
  return bld_capsule;  
}

#if 0
  // not needed, no-op
  static const char builderSetForceNewInstance_doc[] = "Do nothing, we always create a new instance";
  void hdfsBuilderSetForceNewInstance(struct hdfsBuilder * bld);
#endif

static const char builderSetNameNode_doc[] = 
  "Set the HDFS NameNode to connect to";
PyObject *
builderSetNameNode(PyObject * args) {
  struct hdfsBuilder * bld;
  const char * nn;
  if( !PyArg_ParseTuple(args, "O&s", extract_bld, &bld, &nn) ) {
    return NULL;
  }
  hdfsBuilderSetNameNode(bld, nn);
  Py_RETURN_TRUE;
}

static const char builderSetNameNodePort_doc[] = 
  "Set the port of the HDFS NameNode to connect to.";
PyObject *
builderSetNameNodePort(PyObject * args ) {
  struct hdfsBuilder * bld;
  tPort port;
  if( !PyArg_ParseTuple(args, "O&h", extract_bld, &bld, &port) ) {
    return NULL;
  }
  hdfsBuilderSetNameNodePort(bld, port);
  Py_RETURN_TRUE;
}

static const char builderSetUserName_doc[] = 
  "Set the username to use when connecting to the HDFS cluster.";
static PyObject *
builderSetUserName(PyObject * args) {
  struct hdfsBuilder * bld;
  const char * name;
  if( !PyArg_ParseTuple(args, "O&s", extract_bld, &bld, &name) ) {
    return NULL;
  }
  hdfsBuilderSetUserName(bld, name);
  Py_RETURN_TRUE;
}

static const char builderSetKerbTicketCachePath_doc[] = 
  "Set the path to the Kerberos ticket cache to use when connecting to";
static PyObject *
builderSetKerbTicketCachePath(PyObject * args) {
  struct hdfsBuilder * bld;
  const char * name;
  if( !PyArg_ParseTuple(args, "O&s", extract_bld, &bld, &name) ) {
    return NULL;
  }
  hdfsBuilderSetKerbTicketCachePath(bld, name);
  Py_RETURN_TRUE;
}

static const char builderSetToken_doc[] = 
 "Set the token used to authenticate";
static PyObject *
builderSetToken(PyObject * args) {
  struct hdfsBuilder * bld;
  const char * token;
  if( !PyArg_ParseTuple(args, "O&s", extract_bld, &bld, &token) ) {
    return NULL;
  }
  hdfsBuilderSetToken(bld, token);
  Py_RETURN_TRUE;
}

static const char freeBuilder_doc[] = "Free an HDFS builder.";
static PyObject *
freeBuilder(PyObject * args) {
  struct hdfsBuilder * bld;
  if( !PyArg_ParseTuple(args, "O&", extract_bld, &bld) ) {
    return NULL;
  }
  hdfsFreeBuilder(bld);
  Py_RETURN_TRUE;
}

static const char builderConfSetStr_doc[] = 
  "Set a configuration string for an HdfsBuilder.";
static PyObject *
builderConfSetStr(PyObject * args) {
  struct hdfsBuilder * bld;
  const char * key;
  const char * val;
  int erc;
  if( !PyArg_ParseTuple(args, "O&ss", extract_bld, &bld, &key, &val) ) {
    return NULL;
  }
  if( (erc = hdfsBuilderConfSetStr(bld, key, val)) != 0 ) {
    return hdfs_err();
  }
  Py_RETURN_TRUE;
}

static const char confGetStr_doc[] = "Get a configuration string.";
static PyObject *
confGetStr(PyObject * args) {
  const char * key;
  char * val;
  int erc;
  if( !PyArg_ParseTuple(args, "s", &key) ) {
    return NULL;
  }
  if( (erc = hdfsConfGetStr(key, &val)) != 0 ) {
    return hdfs_err();
  }
  return PyUnicode_FromString(val);
}

static const char confGetInt_doc[] = "Get a configuration integer";
static PyObject *
confGetInt(PyObject * args) {
  const char * key;
  int32_t val;
  int erc;
  if( !PyArg_ParseTuple(args, "s", &key) ) {
    return NULL;
  } 
  if( (erc = hdfsConfGetInt(key, &val)) != 0 ) {
    return hdfs_err();
  }
  return PyLong_FromLong((long)val);
}

static const char confStrFree_doc[] = 
  "Free a configuration string found with hdfsConfGetStr";
static PyObject *
confStrFree(PyObject * args) {
  char * key;
  if( !PyArg_ParseTuple(args, "s", &key) ) {
    return NULL;
  } 
  hdfsConfStrFree(key);
  Py_RETURN_TRUE;
}

static const char openFile_doc[] = "Open a hdfs file in given mode";
static PyObject *
openFile(PyObject * args) {
  hdfsFS fs;
  hdfsFile file;
  const char * path;
  int flags;
  int bufferSize;
  short replication;
  tOffset blocksize;
  PyObject * fs_capsule;

  if( !PyArg_ParseTuple(args, "O&siihl", extract_fs, &fs, &path, 
			&flags, &bufferSize, &replication, &blocksize) ) {
    return NULL;
  }
  if( (file = hdfsOpenFile(fs, path, flags, bufferSize, 
			   replication, blocksize)) == NULL ) {
    return hdfs_err();
  }
  if( (fs_capsule = PyCapsule_New(fs, fs_capsule_name, NULL)) == NULL ) {
    return NULL;
  }
  return fs_capsule;  
}

static const char closeFile_doc[] = "Close an open file";
static PyObject *
closeFile(PyObject * args) {
  hdfsFS fs;
  hdfsFile file;
  int erc;
  if( !PyArg_ParseTuple(args, "O&O&", extract_fs, &fs, extract_file, &file) ) {
    return NULL;
  }
  if( (erc = hdfsCloseFile(fs, file)) != 0 ) {
    return hdfs_err();
  }
  Py_RETURN_TRUE;
}

static const char exists_doc[] = 
  "Checks if a given path exsits on the filesystem";
static PyObject *
exists(PyObject * args) {
  hdfsFS fs;
  const char *name;
  int erc;
  if( !PyArg_ParseTuple(args, "O&s", extract_fs, &fs, &name) ) {
    return NULL;
  }
  if( (erc = hdfsExists(fs, name)) != 0 ) {
    return hdfs_err();
  }
  Py_RETURN_TRUE;
}

static const char seek_doc[] = "Seek to given offset in file";
static PyObject *
hdfs_seek(PyObject * args) {
  hdfsFS fs;
  hdfsFile file;
  tOffset pos;
  int erc;
  if( !PyArg_ParseTuple(args, "O&O&l", 
			extract_fs, &fs, extract_file, &file, &pos) ) {
    return NULL;
  }

 if( (erc = hdfsSeek(fs, file, pos)) != 0 ) {
    return hdfs_err();
  }
  Py_RETURN_TRUE;
}

static const char tell_doc[] = "Get the current offset in the file, in bytes";
static PyObject *
hdfs_tell(PyObject * args) {
  hdfsFS fs;
  hdfsFile file;
  tOffset pos;
  if( !PyArg_ParseTuple(args, "O&O&", extract_fs, &fs, extract_file, &file) ) {
    return NULL;
  }

 if( (pos = hdfsTell(fs, file)) == -1 ) {
    return hdfs_err();
  }
  return PyLong_FromLong(pos);
}

static const char read_doc[] = "Read data from an open file";
static PyObject *
hdfs_read(PyObject * args) {
  hdfsFS fs;
  hdfsFile file;
  Py_buffer pybuffer;
  void * buffer = NULL;
  tSize len;
  if( !PyArg_ParseTuple(args, "O&O&y*l", extract_fs, &fs, extract_file, &file, 
			&pybuffer, &len) ) {
    return NULL;
  }

  /* FIXME: convert pybuffer to void * */
  if( buffer == NULL ) {
    PyObject *type = PyErr_NewException("hdfs3py.error", NULL, NULL);
    PyErr_SetString(type, "not implemented");
    return NULL;
  }
 if( (len = hdfsRead(fs, file, buffer, len)) == -1 ) {
    return hdfs_err();
  }
  return PyLong_FromLong(len);
}

static const char write_doc[] = "Write data into an open file";
static PyObject *
hdfs_write(PyObject * args) {
  hdfsFS fs;
  hdfsFile file;
  Py_buffer pybuffer;
  void * buffer = NULL;
  tSize len;
  if( !PyArg_ParseTuple(args, "O&O&y*l", extract_fs, &fs, extract_fs, &file, 
			&pybuffer, &len) ) {
    return NULL;
  }

  /* FIXME: convert pybuffer to void * */
  if( buffer == NULL ) {
    PyObject *type = PyErr_NewException("hdfs3py.error", NULL, NULL);
    PyErr_SetString(type, "not implemented");
    return NULL;
  }

 if( (len = hdfsWrite(fs, file, buffer, len)) == -1 ) {
    return hdfs_err();
  }
  return PyLong_FromLong(len);
}

static const char flush_doc[] = "Flush the data";
static PyObject *
flush(PyObject * args) {
  hdfsFS fs;
  hdfsFile file;
  int erc;
  if( !PyArg_ParseTuple(args, "O&O&", extract_fs, &fs, extract_file, &file) ) {
    return NULL;
  }
 if( (erc = hdfsFlush(fs, file)) == -1 ) {
    return hdfs_err();
  }
  Py_RETURN_TRUE;
}

static const char hFlush_doc[] = 
  "Flush out the data in client's user buffer";
static PyObject *
hFlush(PyObject * args) {
  hdfsFS fs;
  hdfsFile file;
  int erc;
  if( !PyArg_ParseTuple(args, "O&O&", extract_fs, &fs, extract_file, &file) ) {
    return NULL;
  }
 if( (erc = hdfsHFlush(fs, file)) == -1 ) {
    return hdfs_err();
  }
  Py_RETURN_TRUE;
}

static const char sync_doc[] = 
  "Flush out and sync the data in client's user buffer";
static PyObject *
hdfs_sync(PyObject * args) {
  hdfsFS fs;
  hdfsFile file;
  int erc;
  if( !PyArg_ParseTuple(args, "O&O&", extract_fs, &fs, extract_file, &file) ) {
    return NULL;
  }
 if( (erc = hdfsSync(fs, file)) != 0 ) {
    return hdfs_err();
  }
  Py_RETURN_TRUE;
}

static const char available_doc[] = 
  "Number of bytes that can be read from this input stream without blocking";
static PyObject *
available(PyObject * args) {
  hdfsFS fs;
  hdfsFile file;
  int len;
  if( !PyArg_ParseTuple(args, "O&O&", extract_fs, &fs, extract_file, &file) ) {
    return NULL;
  }
 if( (len = hdfsAvailable(fs, file)) == -1 ) {
    return hdfs_err();
  }
  return PyLong_FromLong(len);
}

static const char copy_doc[] = "Copy file from one filesystem to another";
static PyObject *
copy(PyObject * args) {
  hdfsFS srcFS, dstFS;
  const char *src, *dst;
  int erc;
  if( !PyArg_ParseTuple(args, "O&sO&s", 
			extract_fs, &srcFS, &src, 
			extract_fs, &dstFS, &dst) ) {
    return NULL;
  }
 if( (erc = hdfsCopy(srcFS, src, dstFS, dst)) == -1 ) {
    return hdfs_err();
  }
  Py_RETURN_TRUE;
}

static const char move_doc[] = "Move file from one filesystem to another";
static PyObject *
move(PyObject * args) {
  hdfsFS srcFS, dstFS;
  const char *src, *dst;
  int erc;
  if( !PyArg_ParseTuple(args, "O&sO&s", 
			extract_fs, &srcFS, &src, 
			extract_fs, &dstFS, &dst) ) {
    return NULL;
  }
 if( (erc = hdfsMove(srcFS, src, dstFS, dst)) == -1 ) {
    return hdfs_err();
  }
  Py_RETURN_TRUE;
}

static const char delete_doc[] = "Delete file";
static PyObject *
delete(PyObject * args) {
  hdfsFS fs;
  const char *name;
  bool recursive;
  int erc;
  if( !PyArg_ParseTuple(args, "O&sp", extract_fs, &fs, &name, &recursive) ) {
    return NULL;
  }
 if( (erc = hdfsDelete(fs, name, recursive)) == -1 ) {
    return hdfs_err();
  }
  Py_RETURN_TRUE;
}

static const char rename_doc[] = "Rename file";
static PyObject *
hdfs_rename(PyObject * args) {
  hdfsFS fs;
  const char *src, *tgt;
  int erc;
  if( !PyArg_ParseTuple(args, "O&sp", extract_fs, &fs, &src, &tgt) ) {
    return NULL;
  }
 if( (erc = hdfsRename(fs, src, tgt)) == -1 ) {
    return hdfs_err();
  }
  Py_RETURN_TRUE;
}

static const char getWorkingDirectory_doc[] = 
  "Get the current working directory for the given filesystem";
static PyObject *
getWorkingDirectory(PyObject * args) {
  hdfsFS fs;
  Py_buffer *pybuffer;
  char *name, *buffer = NULL;
  size_t len = 0;
  if( !PyArg_ParseTuple(args, "O&y*", extract_fs, &fs, &pybuffer) ) {
    return NULL;
  }
  /* FIXME: convert pybuffer to void * */
  if( buffer == NULL ) {
    PyObject *type = PyErr_NewException("hdfs3py.error", NULL, NULL);
    PyErr_SetString(type, "not implemented");
    return NULL;
  }
  if( (buffer = hdfsGetWorkingDirectory(fs, name, len)) == NULL ) {
    return hdfs_err();
  }
  return PyUnicode_FromString(name);
}

static const char setWorkingDirectory_doc[] = "Set the working directory";
static PyObject *
setWorkingDirectory(PyObject * args) {
  hdfsFS fs;
  char *name;
  int erc;
  if( !PyArg_ParseTuple(args, "O&s", extract_fs, &fs, &name) ) {
    return NULL;
  }
  if( (erc = hdfsSetWorkingDirectory(fs, name)) == -1 ) {
    return hdfs_err();
  }
  Py_RETURN_TRUE;
}

static const char createDirectory_doc[] = 
  "Make the given file and all non-existent parents into directories";
static PyObject *
createDirectory(PyObject * args) {
  hdfsFS fs;
  char *name;
  int erc;
  if( !PyArg_ParseTuple(args, "O&s", extract_fs, &fs, &name) ) {
    return NULL;
  }
  if( (erc = hdfsCreateDirectory(fs, name)) == -1 ) {
    return hdfs_err();
  }
  Py_RETURN_TRUE;
}

static const char setReplication_doc[] = 
  "Set the replication of the specified file to the supplied value";
static PyObject *
setReplication(PyObject * args) {
  hdfsFS fs;
  char *name;
  int16_t replication;
  int erc;
  if( !PyArg_ParseTuple(args, "O&sh", extract_fs, &fs, &name, &replication) ) {
    return NULL;
  }
 if( (erc = hdfsSetReplication(fs, name, replication)) == -1 ) {
    return hdfs_err();
  }
  Py_RETURN_TRUE;
}

/*
    typedef struct {
        tObjectKind mKind;
        char * mName;
        tTime mLastMod;
        tOffset mSize;
        short mReplication;
        tOffset mBlockSize;
        char * mOwner;
        char * mGroup;
        short mPermissions;
        tTime mLastAccess;
    } hdfsFileInfo;
*/

static const char listDirectory_doc[] = 
  "Get list of files/directories for a given";
static PyObject *
listDirectory(PyObject * args) {
  hdfsFS fs;
  char *name;
  hdfsFileInfo * pinfo;
  int nelem;
  int erc;
  if( !PyArg_ParseTuple(args, "O&s", extract_fs, &fs, &name) ) {
    return NULL;
  }

 if( (pinfo = hdfsListDirectory(fs, name, &nelem)) == NULL ) {
    return hdfs_err();
  }
  /* FIXME: return array of tuples or something */
  if( true ) {
    PyObject *type = PyErr_NewException("hdfs3py.error", NULL, NULL);
    PyErr_SetString(type, "not implemented");
    return NULL;
  }
}

static const char getPathInfo_doc[] = 
  "Get information about a path as an hdfsFileInfo struct";
static PyObject *
getPathInfo(PyObject * args) {
  hdfsFS fs;
  char *name;
  hdfsFileInfo * pinfo;
  int erc;
  if( !PyArg_ParseTuple(args, "O&s", extract_fs, &fs, &name) ) {
    return NULL;
  }

 if( (pinfo = hdfsGetPathInfo(fs, name)) == NULL ) {
    return hdfs_err();
  }
  /* FIXME: return hdfsFileInfo class */
  if( true ) {
    PyObject *type = PyErr_NewException("hdfs3py.error", NULL, NULL);
    PyErr_SetString(type, "not implemented");
    return NULL;
  }
}

static int 
extract_fileinfo( PyObject * fileinfo_capsule, void *pout ) {
  struct hdfsFileInfo **pfileinfo = (struct hdfsFileInfo **)pout;
  if( (pout = PyCapsule_GetPointer(fileinfo_capsule, 
				   fileinfo_capsule_name)) == NULL ) {
    hdfs_err();
    return 0;
  }
  *pfileinfo = *(struct hdfsFileInfo **)pout;
  return 1;
}

static const char freeFileInfo_doc[] = 
  "Free up the hdfsFileInfo array (including fields)";
static PyObject *
freeFileInfo(PyObject * args) {
  hdfsFileInfo *pfi;
  int nelem;
  if( !PyArg_ParseTuple(args, "O&si", extract_fileinfo, &pfi, &nelem) ) {
    return NULL;
  }
  hdfsFreeFileInfo(pfi, nelem);
  Py_RETURN_TRUE;
}

static const char getHosts_doc[] = 
  "Get hostnames where a particular block of a file is stored";
static PyObject *
getHosts(PyObject * args) {
  hdfsFS fs;
  char *name;
  tOffset start, len;  
  char ***hosts;
  if( !PyArg_ParseTuple(args, "O&sll", extract_fs, &fs, &name, &start, &len) ) {
    return NULL;
  }

 if( (hosts = hdfsGetHosts(fs, name, start, len)) == NULL ) {
    return hdfs_err();
  }
  /* FIXME: return array */
  if( true ) {
    PyObject *type = PyErr_NewException("hdfs3py.error", NULL, NULL);
    PyErr_SetString(type, "not implemented");
    return NULL;
  }
}

static const char freeHosts_doc[] = 
  "Free up the structure returned by hdfsGetHosts";
static PyObject *
freeHosts(PyObject * args) {
  PyTupleObject *pyhosts;
  char ***hosts = NULL;
  if( !PyArg_ParseTuple(args, "O!", &PyTuple_Type, &pyhosts) ) {
    return NULL;
  }

  /* FIXME: construct array */
  if( hosts == NULL ) {
    PyObject *type = PyErr_NewException("hdfs3py.error", NULL, NULL);
    PyErr_SetString(type, "not implemented");
    return NULL;
  }
  hdfsFreeHosts(hosts);
}

static const char getDefaultBlockSize_doc[] = 
  "Get the default blocksize";
static PyObject *
getDefaultBlockSize(PyObject * args) {
  hdfsFS fs;
  tOffset len;
  if( !PyArg_ParseTuple(args, "O&", extract_fs, &fs) ) {
    return NULL;
  }

  if( (len = hdfsGetDefaultBlockSize(fs)) == -1 ) {
    return  hdfs_err();
  }
  return PyLong_FromLong(len);
}

static const char getCapacity_doc[] = 
  "Return the raw capacity of the filesystem";
static PyObject *
getCapacity(PyObject * args) {
  hdfsFS fs;
  tOffset len;
  if( !PyArg_ParseTuple(args, "O&", extract_fs, &fs) ) {
    return NULL;
  }
  if( (len = hdfsGetCapacity(fs)) == -1 ) {
    return  hdfs_err();
  }
  return PyLong_FromLong(len);
}

static const char getUsed_doc[] = 
  "Return the total raw size of all files in the filesystem";
static PyObject *
getUsed(PyObject * args) {
  hdfsFS fs;
  tOffset len;
  if( !PyArg_ParseTuple(args, "O&", extract_fs, &fs) ) {
    return NULL;
  }
  if( (len = hdfsGetUsed(fs)) == -1 ) {
    return  hdfs_err();
  }
  return PyLong_FromLong(len);
}

static const char chown_doc[] = 
  "Change the user and/or group of a file or directory";
static PyObject *
hdfs_chown(PyObject * args) {
  hdfsFS fs;
  const char *path,  *owner,  *group;
  int erc;
  if( !PyArg_ParseTuple(args, "O&sss", 
			extract_fs, &fs, &path,  &owner,  &group) ) {
    return NULL;
  }
  if( (erc = hdfsChown(fs, path, owner, group)) == -1 ) {
    return  hdfs_err();
  }
  Py_RETURN_TRUE;
}

static const char chmod_doc[] = "Chmod";
static PyObject *
hdfs_chmod(PyObject * args) {
  hdfsFS fs;
  const char *path;
  short mode;
  int erc;
  if( !PyArg_ParseTuple(args, "O&sh", extract_fs, &fs, &path, &mode) ) {
    return NULL;
  }
  if( (erc = hdfsChmod(fs, path, mode)) == -1 ) {
    return  hdfs_err();
  }
  Py_RETURN_TRUE;
}

static const char utime_doc[] = "Utime";
static PyObject *
utime(PyObject * args) {
  hdfsFS fs;
  const char *path;
  tTime mtime, atime;
  int erc;
  if( !PyArg_ParseTuple(args, "O&sll", extract_fs, &fs, &mtime, &atime) ) {
    return NULL;
  }
  if( (erc = hdfsUtime(fs, path, mtime, atime)) == -1 ) {
    return  hdfs_err();
  }
  Py_RETURN_TRUE;
}

static const char truncate_doc[] = 
  "Truncate the file in the indicated path to the indicated size";
static PyObject *
hdfs_truncate(PyObject * args) {
  hdfsFS fs;
  const char *path;
  tOffset pos;
  int shouldWait;
  int erc;
  if( !PyArg_ParseTuple(args, "O&sl", extract_fs, &fs, &pos) ) {
    return NULL;
  }
  if( (erc = hdfsTruncate(fs, path, pos, &shouldWait)) == -1 ) {
    return  hdfs_err();
  }
  if( shouldWait ) 
    Py_RETURN_TRUE;
  Py_RETURN_FALSE;
}

static const char getDelegationToken_doc[] = 
  "Get a delegation token from namenode";
static PyObject *
getDelegationToken(PyObject * args) {
  hdfsFS fs;
  const char *name;
  char * token;
  if( !PyArg_ParseTuple(args, "O&s", extract_fs, &fs, &name) ) {
    return NULL;
  }
  if( (token = hdfsGetDelegationToken(fs, name)) == NULL ) {
    return  hdfs_err();
  }
  return PyUnicode_FromString(name);
}

static const char freeDelegationToken_doc[] = 
  "Free a delegation token";
static PyObject *
freeDelegationToken(PyObject * args) {
  hdfsFS fs;
  char * token;
  if( !PyArg_ParseTuple(args, "s", &token) ) {
    return NULL;
  }
  hdfsFreeDelegationToken(token);
  Py_RETURN_TRUE;
}

static const char renewDelegationToken_doc[] = "Renew a delegation token";
static PyObject *
renewDelegationToken(PyObject * args) {
  hdfsFS fs;
  char * token;
  long erc;
  if( !PyArg_ParseTuple(args, "O&s", extract_fs, &fs, &token) ) {
    return NULL;
  }
  if( (erc = hdfsRenewDelegationToken(fs, token)) == -1 ) {
    return  hdfs_err();
  }
  Py_RETURN_TRUE;
}

static const char cancelDelegationToken_doc[] = 
  "Cancel a delegation token";
static PyObject *
cancelDelegationToken(PyObject * args) {
  hdfsFS fs;
  char * token;
  long erc;
  if( !PyArg_ParseTuple(args, "O&s", extract_fs, &fs, &token) ) {
    return NULL;
  }
  if( (erc = hdfsCancelDelegationToken(fs, token)) == -1 ) {
    return  hdfs_err();
  }
  Py_RETURN_TRUE;
}

/* 
    typedef struct Namenode {
        char * rpc_addr;
        char * http_addr;
    } Namenode;
*/

static const char getHANamenodes_doc[] = 
  "If hdfs is configured with HA namenode, "
  "return all namenode information as an array, else NULL"
  " (config is optional 3rd parameter)";
static PyObject *
getHANamenodes(PyObject * args) {
  const char * nameservice, *config = NULL;
  int len;
  Namenode *nodes;
  if( !PyArg_ParseTuple(args, "s|s", &nameservice, &config) ) {
    return NULL;
  }
  if( config != NULL ) {
    if( (nodes = hdfsGetHANamenodesWithConfig(config, nameservice, &len)) == NULL ) {
      return  hdfs_err();
    }
  } else {
    if( (nodes = hdfsGetHANamenodes(nameservice, &len)) == NULL ) {
      return  hdfs_err();
    }
  }
  /* FIXME: construct array */
  if( true ) {
    PyObject *type = PyErr_NewException("hdfs3py.error", NULL, NULL);
    PyErr_SetString(type, "not implemented");
    return NULL;
  }
}

static const char freeNamenodeInformation_doc[] = 
  "Free the array returned by hdfsGetConfiguredNamenodes";
static PyObject *
freeNamenodeInformation(PyObject * args) {
  PyTupleObject *pynodes;
  Namenode * nodes = NULL;
  int len = 0;
  if( !PyArg_ParseTuple(args, "O!", &PyTuple_Type, &pynodes) ) {
    return NULL;
  }

  /* FIXME: construct array */
  if( nodes == NULL ) {
    PyObject *type = PyErr_NewException("hdfs3py.error", NULL, NULL);
    PyErr_SetString(type, "not implemented");
    return NULL;
  }

  hdfsFreeNamenodeInformation(nodes, len);
}


/*
    typedef struct BlockLocation {
        int corrupt;
        int numOfNodes;
        char ** hosts;
        char ** names;
        char ** topologyPaths;
        tOffset length;
        tOffset offset;
    } BlockLocation;
*/

static const char getFileBlockLocations_doc[] = 
  "Get an array containing hostnames, offset and size "
  "of portions of the given file";
static PyObject *
getFileBlockLocations(PyObject * args) {
  hdfsFS fs;
  const char *name;
  tOffset start;
  tOffset len;
  BlockLocation *pblocks;
  int nblocks;
  if( !PyArg_ParseTuple(args, "O&s", extract_fs, &fs, &name, &start, &len) ) {
    return NULL;
  }
  if( (pblocks = hdfsGetFileBlockLocations(fs, name, 
					   start, len, &nblocks)) == NULL ) {
    return  hdfs_err();
  }
  /* FIXME: construct array */
  if( true ) {
    PyObject *type = PyErr_NewException("hdfs3py.error", NULL, NULL);
    PyErr_SetString(type, "not implemented");
    return NULL;
  }
}

static const char freeFileBlockLocations_doc[] = 
  "Free the BlockLocation array returned by hdfsGetFileBlockLocations";
static PyObject *
freeFileBlockLocations(PyObject * args) {
  BlockLocation *pblocks;
  int nblocks;
  PyTupleObject *pynodes;
  if( !PyArg_ParseTuple(args, "O!", &PyTuple_Type, &pynodes) ) {
    return NULL;
  }
  /* FIXME: construct array */
  if( true ) {
    PyObject *type = PyErr_NewException("hdfs3py.error", NULL, NULL);
    PyErr_SetString(type, "not implemented");
    return NULL;
  }
  hdfsFreeFileBlockLocations(pblocks, nblocks);
  Py_RETURN_TRUE;
}

enum { METH_KWARGS = METH_VARARGS | METH_KEYWORDS };

static PyMethodDef methods[] = {
  { "getLastError", (PyCFunction)getLastError, METH_NOARGS, PyDoc_STR(getLastError_doc) }, 
  { "fileIsOpenForRead", (PyCFunction)fileIsOpenForRead, METH_VARARGS, PyDoc_STR(fileIsOpenForRead_doc) }, 
  { "fileIsOpenForWrite", (PyCFunction)fileIsOpenForWrite, METH_VARARGS, PyDoc_STR(fileIsOpenForWrite_doc) }, 
  { "connect", (PyCFunction)connect, METH_KWARGS, PyDoc_STR(connect_doc) }, 
  { "builderConnect", (PyCFunction)builderConnect, METH_VARARGS, PyDoc_STR(builderConnect_doc) }, 
  { "newBuilder", (PyCFunction)newBuilder, METH_NOARGS, PyDoc_STR(newBuilder_doc) }, 
  { "builderSetNameNode", (PyCFunction)builderSetNameNode, METH_VARARGS, PyDoc_STR(builderSetNameNode_doc) }, 
  { "builderSetNameNodePort", (PyCFunction)builderSetNameNodePort, METH_VARARGS, PyDoc_STR(builderSetNameNodePort_doc) }, 
  { "builderSetUserName", (PyCFunction)builderSetUserName, METH_VARARGS, PyDoc_STR(builderSetUserName_doc) }, 
  { "builderSetKerbTicketCachePath", (PyCFunction)builderSetKerbTicketCachePath, METH_VARARGS, PyDoc_STR(builderSetKerbTicketCachePath_doc) }, 
  { "builderSetToken", (PyCFunction)builderSetToken, METH_VARARGS, PyDoc_STR(builderSetToken_doc) }, 
  { "freeBuilder", (PyCFunction)freeBuilder, METH_VARARGS, PyDoc_STR(freeBuilder_doc) }, 
  { "builderConfSetStr", (PyCFunction)builderConfSetStr, METH_VARARGS, PyDoc_STR(builderConfSetStr_doc) }, 
  { "confGetStr", (PyCFunction)confGetStr, METH_VARARGS, PyDoc_STR(confGetStr_doc) }, 
  { "confGetInt", (PyCFunction)confGetInt, METH_VARARGS, PyDoc_STR(confGetInt_doc) }, 
  { "confStrFree", (PyCFunction)confStrFree, METH_VARARGS, PyDoc_STR(confStrFree_doc) }, 
  { "disconnect", (PyCFunction)disconnect, METH_VARARGS, PyDoc_STR(disconnect_doc) }, 
  { "openFile", (PyCFunction)openFile, METH_VARARGS, PyDoc_STR(openFile_doc) }, 
  { "closeFile", (PyCFunction)closeFile, METH_VARARGS, PyDoc_STR(closeFile_doc) }, 
  { "exists", (PyCFunction)exists, METH_VARARGS, PyDoc_STR(exists_doc) }, 
  { "seek", (PyCFunction)hdfs_seek, METH_VARARGS, PyDoc_STR(seek_doc) }, 
  { "tell", (PyCFunction)hdfs_tell, METH_VARARGS, PyDoc_STR(tell_doc) }, 
  { "read", (PyCFunction)hdfs_read, METH_VARARGS, PyDoc_STR(read_doc) }, 
  { "write", (PyCFunction)hdfs_write, METH_VARARGS, PyDoc_STR(write_doc) }, 
  { "flush", (PyCFunction)flush, METH_VARARGS, PyDoc_STR(flush_doc) }, 
  { "hFlush", (PyCFunction)hFlush, METH_VARARGS, PyDoc_STR(hFlush_doc) }, 
  { "sync", (PyCFunction)hdfs_sync, METH_VARARGS, PyDoc_STR(sync_doc) }, 
  { "available", (PyCFunction)available, METH_VARARGS, PyDoc_STR(available_doc) }, 
  { "copy", (PyCFunction)copy, METH_VARARGS, PyDoc_STR(copy_doc) }, 
  { "move", (PyCFunction)move, METH_VARARGS, PyDoc_STR(move_doc) }, 
  { "delete", (PyCFunction)delete, METH_VARARGS, PyDoc_STR(delete_doc) }, 
  { "rename", (PyCFunction)hdfs_rename, METH_VARARGS, PyDoc_STR(rename_doc) }, 
  { "getWorkingDirectory", (PyCFunction)getWorkingDirectory, METH_VARARGS, PyDoc_STR(getWorkingDirectory_doc) }, 
  { "setWorkingDirectory", (PyCFunction)setWorkingDirectory, METH_VARARGS, PyDoc_STR(setWorkingDirectory_doc) }, 
  { "createDirectory", (PyCFunction)createDirectory, METH_VARARGS, PyDoc_STR(createDirectory_doc) }, 
  { "setReplication", (PyCFunction)setReplication, METH_VARARGS, PyDoc_STR(setReplication_doc) }, 
  { "listDirectory", (PyCFunction)listDirectory, METH_VARARGS, PyDoc_STR(listDirectory_doc) }, 
  { "getPathInfo", (PyCFunction)getPathInfo, METH_VARARGS, PyDoc_STR(getPathInfo_doc) }, 
  { "freeFileInfo", (PyCFunction)freeFileInfo, METH_VARARGS, PyDoc_STR(freeFileInfo_doc) }, 
  { "getHosts", (PyCFunction)getHosts, METH_VARARGS, PyDoc_STR(getHosts_doc) }, 
  { "freeHosts", (PyCFunction)freeHosts, METH_VARARGS, PyDoc_STR(freeHosts_doc) }, 
  { "getDefaultBlockSize", (PyCFunction)getDefaultBlockSize, METH_VARARGS, PyDoc_STR(getDefaultBlockSize_doc) }, 
  { "getCapacity", (PyCFunction)getCapacity, METH_VARARGS, PyDoc_STR(getCapacity_doc) }, 
  { "getUsed", (PyCFunction)getUsed, METH_VARARGS, PyDoc_STR(getUsed_doc) }, 
  { "chown", (PyCFunction)hdfs_chown, METH_VARARGS, PyDoc_STR(chown_doc) }, 
  { "chmod", (PyCFunction)hdfs_chmod, METH_VARARGS, PyDoc_STR(chmod_doc) }, 
  { "utime", (PyCFunction)utime, METH_VARARGS, PyDoc_STR(utime_doc) }, 
  { "truncate", (PyCFunction)hdfs_truncate, METH_VARARGS, PyDoc_STR(truncate_doc) }, 
  { "getDelegationToken", (PyCFunction)getDelegationToken, METH_VARARGS, PyDoc_STR(getDelegationToken_doc) }, 
  { "freeDelegationToken", (PyCFunction)freeDelegationToken, METH_VARARGS, PyDoc_STR(freeDelegationToken_doc) }, 
  { "renewDelegationToken", (PyCFunction)renewDelegationToken, METH_VARARGS, PyDoc_STR(renewDelegationToken_doc) }, 
  { "cancelDelegationToken", (PyCFunction)cancelDelegationToken, METH_VARARGS, PyDoc_STR(cancelDelegationToken_doc) }, 
  { "getHANamenodes", (PyCFunction)getHANamenodes, METH_VARARGS, PyDoc_STR(getHANamenodes_doc) }, 
  { "freeNamenodeInformation", (PyCFunction)freeNamenodeInformation, METH_VARARGS, PyDoc_STR(freeNamenodeInformation_doc) }, 
  { "getFileBlockLocations", (PyCFunction)getFileBlockLocations, METH_VARARGS, PyDoc_STR(getFileBlockLocations_doc) }, 
  { "freeFileBlockLocations", (PyCFunction)freeFileBlockLocations, METH_VARARGS, PyDoc_STR(freeFileBlockLocations_doc) }, 
  { NULL, NULL, 0, NULL },
};

