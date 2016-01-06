#include <Python.h>
#include <memoryobject.h>
#include <stdbool.h>
#include <unistd.h>

#include "../src/client/hdfs.h"

static const char module_name[] = "hdfs3py";
static const char bld_capsule_name[] = "hdfs3py.hdfsBuilder";
static const char file_capsule_name[] = "hdfs3py.hdfsFile";
static const char fileinfo_capsule_name[] = "hdfs3py.hdfsFileInfo";
static const char fs_capsule_name[] = "hdfs3py.hdfsFS";

static PyObject *
hdfs_err_msg( const char *msg ) {
  PyObject *type = PyErr_NewException("hdfs3py.error", NULL, NULL);
  PyErr_SetString(type, msg);
  return NULL;
}  

static PyObject *
hdfs_err() {
  const char *msg = hdfsGetLastError();
  return hdfs_err_msg(msg);
}  

static const char getLastError_doc[] =
  "Return error information of last failed operation";
static PyObject *
getLastError() {
  return PyUnicode_FromString(hdfsGetLastError());
}

static int 
extract_fs( PyObject * fs_capsule, void *pout ) {
  hdfsFS *fs;
  if( (fs = PyCapsule_GetPointer(fs_capsule, fs_capsule_name)) == NULL ) {
    hdfs_err();
    return 0;
  }
  *(hdfsFS**)pout = fs;
  return 1;
}

static int 
extract_file( PyObject * file_capsule, void *pout ) {
  hdfsFile *file;
  if( (file = PyCapsule_GetPointer(file_capsule, file_capsule_name)) == NULL ) {
    hdfs_err();
    return 0;
  }
  *(hdfsFile**)pout = file;
  return 1;
}

static const char fileIsOpenForRead_doc[] = 
  "Determine if a file is open for read";
static PyObject *
fileIsOpenForRead(PyObject *self, PyObject *args) {
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
fileIsOpenForWrite(PyObject *self, PyObject *args) {
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
connect(PyObject *self, PyObject *args, PyObject *keywords) {
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
disconnect(PyObject *self, PyObject *args) {
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
  struct hdfsBuilder *bld;
  if( (bld = PyCapsule_GetPointer(bld_capsule, bld_capsule_name)) == NULL ) {
    hdfs_err();
    return 0;
  }
  *(struct hdfsBuilder **)pout = bld;
  return 1;
}

static const char builderConnect_doc[] = 
  "Connect to HDFS using the parameters defined by the builder";
static PyObject *
builderConnect(PyObject *self, PyObject *args) {
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
builderSetNameNode(PyObject *self, PyObject *args) {
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
builderSetNameNodePort(PyObject *self, PyObject *args ) {
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
builderSetUserName(PyObject *self, PyObject *args) {
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
builderSetKerbTicketCachePath(PyObject *self, PyObject *args) {
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
builderSetToken(PyObject *self, PyObject *args) {
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
freeBuilder(PyObject *self, PyObject *args) {
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
builderConfSetStr(PyObject *self, PyObject *args) {
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
confGetStr(PyObject *self, PyObject *args) {
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
confGetInt(PyObject *self, PyObject *args) {
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
confStrFree(PyObject *self, PyObject *args) {
  char * key;
  if( !PyArg_ParseTuple(args, "s", &key) ) {
    return NULL;
  } 
  hdfsConfStrFree(key);
  Py_RETURN_TRUE;
}

static const char openFile_doc[] = "Open a hdfs file in given mode";
static PyObject *
openFile(PyObject *self, PyObject *args) {
  hdfsFS fs;
  hdfsFile file;
  const char * path;
  int flags;
  int bufferSize;
  short replication;
  tOffset blocksize;
  PyObject * capsule;

  if( !PyArg_ParseTuple(args, "O&siihl", extract_fs, &fs, &path, 
			&flags, &bufferSize, &replication, &blocksize) ) {
    return NULL;
  }
  if( (file = hdfsOpenFile(fs, path, flags, bufferSize, 
			   replication, blocksize)) == NULL ) {
    return hdfs_err();
  }
  if( (capsule = PyCapsule_New(file, file_capsule_name, NULL)) == NULL ) {
    return NULL;
  }
  return capsule;  
}

static const char closeFile_doc[] = "Close an open file";
static PyObject *
closeFile(PyObject *self, PyObject *args) {
  hdfsFS fs;
  hdfsFile file;
  int erc;
  if( !PyArg_ParseTuple(args, "O&O&", extract_fs, &fs, extract_file, &file) ) {
    return NULL;
  }
  if( !fs ) {
    return hdfs_err_msg("fs is null");
  }
  if( (erc = hdfsCloseFile(fs, file)) != 0 ) {
    return hdfs_err();
  }
  Py_RETURN_TRUE;
}

static const char exists_doc[] = 
  "Checks if a given path exsits on the filesystem";
static PyObject *
exists(PyObject *self, PyObject *args) {
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
hdfs_seek(PyObject *self, PyObject *args) {
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
hdfs_tell(PyObject *self, PyObject *args) {
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
hdfs_read(PyObject *self, PyObject *args) {
  hdfsFS fs;
  hdfsFile file;
  PyObject *buffer;
  tSize len;
  if( !PyArg_ParseTuple(args, "O&O&l", extract_fs, &fs, 
			               extract_file, &file, &len) ) {
    return NULL;
  }

  if( (buffer = PyBytes_FromStringAndSize((char *)NULL, len)) == NULL ) {
    return NULL;
  }

  if( (len = hdfsRead(fs, file, PyBytes_AS_STRING(buffer), len)) == -1 ) {
    Py_DECREF(buffer);
    return hdfs_err();
  }
  return buffer;
}

static const char write_doc[] = "Write data into an open file";
static PyObject *
hdfs_write(PyObject *self, PyObject *args) {
  hdfsFS fs;
  hdfsFile file;
  PyObject *input;
  tSize len;
  if( !PyArg_ParseTuple(args, "O&O&Ol", extract_fs, &fs, extract_file, &file, 
			&input, &len) ) {
    return NULL;
  }

  if( !fs ) {  // Who knows why?  PyArg_ParseTuple is setting fs to NULL. 
    PyObject *o;
    if( (o = PyTuple_GetItem(args, 0)) == NULL ) {
      return NULL;
    }
    if( 0 == extract_fs(o, &fs) ) {
      static const char msg[] = "fs is NULL";
      return hdfs_err_msg(msg);
    }
  }

  char *buffer = PyBytes_AsString(input);
  if( !buffer ) {
    return NULL;
  }

  if( (len = hdfsWrite(fs, file, buffer, len)) == -1 ) {
    fprintf(stderr, "write failed, padre\n");
    return hdfs_err();
  }
  return PyLong_FromLong(len);
}

static const char flush_doc[] = "Flush the data";
static PyObject *
flush(PyObject *self, PyObject *args) {
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
hFlush(PyObject *self, PyObject *args) {
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
hdfs_sync(PyObject *self, PyObject *args) {
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
available(PyObject *self, PyObject *args) {
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
copy(PyObject *self, PyObject *args) {
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
move(PyObject *self, PyObject *args) {
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
hdfs_delete(PyObject *self, PyObject *args) {
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
hdfs_rename(PyObject *self, PyObject *args) {
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
getWorkingDirectory(PyObject *self, PyObject *args) {
  hdfsFS fs;
  PyObject *buffer;
  char *name;
  size_t len = PATH_MAX;
  if( !PyArg_ParseTuple(args, "O&", extract_fs, &fs) ) {
    return NULL;
  }

  if( (buffer = PyBytes_FromStringAndSize((char *)NULL, len)) == NULL ) {
    return NULL;
  }

  if( hdfsGetWorkingDirectory(fs, PyBytes_AS_STRING(buffer), len) == NULL ) {
    Py_DECREF(buffer);
    return hdfs_err();
  }
  return buffer;
}

static const char setWorkingDirectory_doc[] = "Set the working directory";
static PyObject *
setWorkingDirectory(PyObject *self, PyObject *args) {
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
createDirectory(PyObject *self, PyObject *args) {
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
setReplication(PyObject *self, PyObject *args) {
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

static PyTypeObject * hdfsFileInfo_Type = NULL;

static void 
hdfsFileInfo_NewType() {
  static PyStructSequence_Field fields[] = 
    { 
      { "mKind", "file or directory" }, 
      { "mName", "the name of the file" }, 
      { "mLastMod", "the last modification time for the file in seconds" }, 
      { "mSize", "the size of the file in bytes" }, 
      { "mReplication", "the count of replicas" }, 
      { "mBlockSize", "the block size for the file" }, 
      { "mOwner", "the owner of the file" }, 
      { "mGroup", "the group associated with the file" }, 
      { "mPermissions", "the permissions associated with the file" }, 
      { "mLastAccess", "the last access time for the file in seconds" }, 
      { NULL, NULL }
    };
  static PyStructSequence_Desc desc = 
    { .name = "hdfsFileInfo", 
      .doc = "Information about a file/directory", 
      .fields = fields, 
      .n_in_sequence = sizeof(fields)/sizeof(fields[0]) - 1
    };

  hdfsFileInfo_Type = PyStructSequence_NewType(&desc);
}

static PyObject *
hdfsFileInfo_New(const hdfsFileInfo *info) {
  enum {mKind, mName, mLastMod, mSize, mReplication, mBlockSize, 
	mOwner, mGroup, mPermissions, mLastAccess};
  if( hdfsFileInfo_Type == NULL ) {
    hdfsFileInfo_NewType();
  }
  PyObject* obj = PyStructSequence_New(hdfsFileInfo_Type);
  const char kind = info->mKind;
  PyObject *e[1 + mLastAccess];
  ssize_t i;

  e[mKind] = PyUnicode_FromStringAndSize(&kind, 1);
  e[mName] = PyUnicode_FromString(info->mName);
  e[mLastMod] = PyLong_FromLong(info->mLastMod);
  e[mSize] = PyLong_FromLong(info->mSize);
  e[mReplication] = PyLong_FromLong(info->mReplication);
  e[mBlockSize] = PyLong_FromLong(info->mBlockSize);
  e[mOwner] = PyUnicode_FromString(info->mOwner);
  e[mGroup] = PyUnicode_FromString(info->mGroup);
  e[mPermissions] = PyLong_FromLong(info->mPermissions);
  e[mLastAccess] = PyLong_FromLong(info->mLastAccess);

  for( i=0; i < sizeof(e)/sizeof(e[0]); i++ ) {
    if( e[i] == NULL ) {
      PyObject *type = PyErr_NewException("hdfs3py.error", NULL, NULL);
      PyErr_SetString(type, "logic error");
      return NULL;
    }
    PyStructSequence_SetItem(obj, i, e[i]);
  } 
  return obj;
}
				   
static const char listDirectory_doc[] = 
  "Get list of files/directories for a given";
static PyObject *
listDirectory(PyObject *self, PyObject *args) {
  hdfsFS fs;
  char *name;
  hdfsFileInfo *pinfo;
  int nelem;
  int i, erc;
  PyObject *output;
  if( !PyArg_ParseTuple(args, "O&s", extract_fs, &fs, &name) ) {
    return NULL;
  }

  if( (pinfo = hdfsListDirectory(fs, name, &nelem)) == NULL ) {
    return hdfs_err();
  }
  
  if( (output = PyTuple_New(nelem)) == NULL ) {
    return NULL;
  }
 
  for( i=0; i < nelem; i++ ) {
    PyObject *elem;
    if( (elem = hdfsFileInfo_New(pinfo + i)) == NULL ){
      PyObject *type = PyErr_NewException("hdfs3py.error", NULL, NULL);
      PyErr_SetString(type, "logic error");
      return NULL;
    }
    if( 0 != PyTuple_SetItem(output, i, elem) ) {
      return NULL;
    }
  }
  return output;
}

static const char getPathInfo_doc[] = 
  "Get information about a path as an hdfsFileInfo struct";
static PyObject *
getPathInfo(PyObject *self, PyObject *args) {
  hdfsFS fs;
  char *name;
  hdfsFileInfo * info;
  int erc;
  PyObject *output;

  if( !PyArg_ParseTuple(args, "O&s", extract_fs, &fs, &name) ) {
    return NULL;
  }

  if( (info = hdfsGetPathInfo(fs, name)) == NULL ) {
    return hdfs_err();
  }

  if( (output = hdfsFileInfo_New(info)) == NULL ){
    PyObject *type = PyErr_NewException("hdfs3py.error", NULL, NULL);
    PyErr_SetString(type, "logic error");
    return NULL;
  }
  if( 0 != PyTuple_SetItem(output, 0, output) ) {
    return NULL;
  }
  return output;
}

#if defined(__cplusplus)
# define STRUCT
#else
# define STRUCT struct
#endif

static int 
extract_fileinfo( PyObject * fileinfo_capsule, void *pout ) {
  STRUCT hdfsFileInfo **pfileinfo = (STRUCT hdfsFileInfo **)pout;
  if( (pout = PyCapsule_GetPointer(fileinfo_capsule, 
				   fileinfo_capsule_name)) == NULL ) {
    hdfs_err();
    return 0;
  }
  *pfileinfo = *(STRUCT hdfsFileInfo **)pout;
  return 1;
}

static const char freeFileInfo_doc[] = 
  "Free up the hdfsFileInfo array (including fields)";
static PyObject *
freeFileInfo(PyObject *self, PyObject *args) {
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
getHosts(PyObject *self, PyObject *args) {
  hdfsFS fs;
  char *name;
  tOffset i, nelem, start, len;  
  char ***hosts;
  PyObject *output;
  if( !PyArg_ParseTuple(args, "O&sll", extract_fs, &fs, &name, &start, &len) ) {
    return NULL;
  }

  if( (hosts = hdfsGetHosts(fs, name, start, len)) == NULL ) {
    return hdfs_err();
  }

  for( nelem=0; hosts + nelem != NULL; nelem++ )
    ;

  if( (output = PyTuple_New(nelem)) == NULL ) {
    return NULL;
  }
 
  for( i=0; i < nelem; i++ ) {
    int j, nhosts = 0;
    PyObject *row;
    while( hosts[i] + nhosts ) {
      nhosts++;
    }
    if( (row = PyTuple_New(nhosts)) == NULL ) {
      return NULL;
    }
    for( j=0; j < nhosts; j++ ) {
      PyObject *elem;
      if( (elem = PyUnicode_FromString(hosts[i][j])) == NULL ){
	PyObject *type = PyErr_NewException("hdfs3py.error", NULL, NULL);
	PyErr_SetString(type, "logic error");
	return NULL;
      }
      if( 0 != PyTuple_SetItem(row, j, elem) ) {
	return NULL;
      }
    }      

    if( 0 != PyTuple_SetItem(output, i, row) ) {
      return NULL;
    }
  }
  return output;
}

static const char freeHosts_doc[] = 
  "Free up the structure returned by hdfsGetHosts";
static PyObject *
freeHosts(PyObject *self, PyObject *args) {
  PyObject *pyhosts;
  int i, j, nhosts;
  char ***hosts = NULL;
  if( !PyArg_ParseTuple(args, "O!", &PyTuple_Type, &pyhosts) ) {
    return NULL;
  }

  nhosts = PyTuple_Size(pyhosts);
  if( (hosts = (char***) calloc( 1 + nhosts, sizeof(char**))) == NULL ) {
    return PyErr_NoMemory();
  }
  for( i=0; i < nhosts; i++ ) {
    PyObject *row = PyTuple_GetItem(pyhosts, i);
    int nelem = PyTuple_Size(row);
    if( row == NULL ) return NULL;
    if( (hosts[i] = (char**)calloc(1 + nelem, sizeof(char*))) == NULL ) {
      return PyErr_NoMemory();
    }
    for( j=0; j < nelem; j++ ) {
      PyObject *elem = PyTuple_GetItem(row, j);
      if( (hosts[i][j] = strdup(PyBytes_AsString(elem))) == NULL ) {
	return PyErr_NoMemory();
      }
    }
  }
      
  hdfsFreeHosts(hosts);

  for( i=0; hosts != NULL && i < nhosts; i++ ) {
    for( j=0; hosts[i] != NULL && hosts[i][j] != NULL; j++ ) {
      free(hosts[i][j]);
    }
    free(hosts[i]);
  }
  free(hosts);

  Py_RETURN_TRUE;
}

static const char getDefaultBlockSize_doc[] = 
  "Get the default blocksize";
static PyObject *
getDefaultBlockSize(PyObject *self, PyObject *args) {
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
getCapacity(PyObject *self, PyObject *args) {
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
getUsed(PyObject *self, PyObject *args) {
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
hdfs_chown(PyObject *self, PyObject *args) {
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
hdfs_chmod(PyObject *self, PyObject *args) {
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
utime(PyObject *self, PyObject *args) {
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
hdfs_truncate(PyObject *self, PyObject *args) {
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
getDelegationToken(PyObject *self, PyObject *args) {
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
freeDelegationToken(PyObject *self, PyObject *args) {
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
renewDelegationToken(PyObject *self, PyObject *args) {
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
cancelDelegationToken(PyObject *self, PyObject *args) {
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

static PyTypeObject * Namenode_Type = NULL;

static void 
Namenode_NewType() {
  static PyStructSequence_Field fields[] = 
    { { "rpc_addr", "namenode rpc address and port, such as 'host:9000'" },
      { "http_addr", "namenode http address and port, such as 'host:50070'" }
    };
  static PyStructSequence_Desc desc = 
    { .name = "Namenode", 
      .doc = "RPC information for HTTP nodes", 
      .fields = fields, 
      .n_in_sequence = sizeof(fields)/sizeof(fields[0]) - 1
    };
  Namenode_Type = PyStructSequence_NewType(&desc);
}

static PyObject *
Namenode_New(const Namenode *info) {
  enum {rpc_addr, http_addr};
  if( Namenode_Type == NULL ) {
    Namenode_NewType();
  }
  PyObject* obj = PyStructSequence_New(Namenode_Type);
  PyObject *e[1 + http_addr];
  ssize_t i;

  e[rpc_addr]  = PyUnicode_FromString(info->rpc_addr);
  e[http_addr] = PyUnicode_FromString(info->http_addr);

  for( i=0; i < sizeof(e)/sizeof(e[0]); i++ ) {
    if( e[i] == NULL ) {
      PyObject *type = PyErr_NewException("hdfs3py.error", NULL, NULL);
      PyErr_SetString(type, "logic error");
      return NULL;
    }
    PyStructSequence_SetItem(obj, i, e[i]);
  } 
  return obj;
}

static const char getHANamenodes_doc[] = 
  "If hdfs is configured with HA namenode, "
  "return all namenode information as an array, else NULL"
  " (config is optional 3rd parameter)";
static PyObject *
getHANamenodes(PyObject *self, PyObject *args) {
  const char * nameservice, *config = NULL;
  int i, len;
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

  if( Namenode_Type == NULL ) {
    Namenode_NewType();
  }

  PyObject *output;
  if( (output = PyTuple_New(len)) == NULL ) {
    return NULL;
  }

  for( i=0; i < len; i++ ) {
    PyObject *elem;
    if( (elem = Namenode_New(nodes + i)) == NULL ) {
      PyObject *type = PyErr_NewException("hdfs3py.error", NULL, NULL);
      PyErr_SetString(type, "could not create namenode");
      return NULL;
    }

    if( 0 != PyTuple_SetItem(output, i, elem) ) {
      return NULL;
    }
  }
  return output;
}

static const char freeNamenodeInformation_doc[] = 
  "Free the array returned by hdfsGetConfiguredNamenodes";
static PyObject *
freeNamenodeInformation(PyObject *self, PyObject *args) {
  PyObject *pynodes;
  Namenode * nodes = NULL;
  int i, len = 0;
  if( !PyArg_ParseTuple(args, "O!", &PyTuple_Type, &pynodes) ) {
    return NULL;
  }

  len = PyTuple_Size(pynodes);

  if( (nodes = (Namenode*)calloc(len, sizeof(Namenode))) == NULL ) {
    return PyErr_NoMemory();
  }
  
  for( i=0; i < len; i++ ) {
    PyObject *elem;
    if( (elem = PyTuple_GetItem(pynodes, i)) == NULL ) {
      return NULL;
    }    
    nodes[i].rpc_addr =  PyBytes_AsString(PyTuple_GetItem(elem, 0));
    nodes[i].http_addr = PyBytes_AsString(PyTuple_GetItem(elem, 1));
  }

  hdfsFreeNamenodeInformation(nodes, len);

  free(nodes);
  Py_RETURN_TRUE;
}

static PyTypeObject * BlockLocation_Type = NULL;

static void 
BlockLocation_NewType()  {
  static PyStructSequence_Field fields[] = 
    { { "corrupt", "If the block is corrupt" }, 
      { "numOfNodes", "Number of Datanodes which keep the block" }, 
      { "hosts", "Datanode hostnames" }, 
      { "names", "Datanode IP:xferPort for accessing the block" }, 
      { "topologyPaths", "Full path name in network topology" }, 
      { "length", "block length, may be 0 for the last block" }, 
      { "offset", "Offset of the block in the file" }, 
    };
  static PyStructSequence_Desc desc = 
    { .name = "BlockLocation", 
      .doc = "hostnames, offset and size of portions of a file", 
      .fields = fields, 
      .n_in_sequence = sizeof(fields)/sizeof(fields[0]) - 1
    };
  BlockLocation_Type = PyStructSequence_NewType(&desc);
}

static PyObject *
BlockLocation_SetRow( char **input ) {
  PyObject *output;
  int i, nelem;
  for( nelem=0; input + nelem != NULL; nelem++ )
    ;
  if( (output = PyTuple_New(nelem)) == NULL ) {
    return NULL;
  }
  for( i=0; i < nelem; i++ ) {
    PyObject *elem = PyUnicode_FromString(input[i]);
    if( 0 != PyTuple_SetItem(output, i, elem) ) {
      return NULL;
    }
  }
  return output;
}

static PyObject *
BlockLocation_New(const BlockLocation *info) {
  enum {corrupt, numOfNodes, hosts, names, topologyPaths, length, offset};
  if( BlockLocation_Type == NULL ) {
    BlockLocation_NewType();
  }
  PyObject* obj = PyStructSequence_New(BlockLocation_Type);
  PyObject *e[1 + offset];
  ssize_t i;

  e[corrupt]       = PyLong_FromLong(info->corrupt);
  e[numOfNodes]    = PyLong_FromLong(info->numOfNodes);
  e[length]        = PyLong_FromLong(info->length);
  e[offset]        = PyLong_FromLong(info->offset);

  e[hosts] = BlockLocation_SetRow(info->hosts);
  e[names] = BlockLocation_SetRow(info->names);
  e[topologyPaths] = BlockLocation_SetRow(info->topologyPaths);

  for( i=0; i < sizeof(e)/sizeof(e[0]); i++ ) {
    if( e[i] == NULL ) {
      PyObject *type = PyErr_NewException("hdfs3py.error", NULL, NULL);
      PyErr_SetString(type, "logic error");
      return NULL;
    }
    PyStructSequence_SetItem(obj, i, e[i]);
  } 
  return obj;
}

static char **
BlockLocation_GetRow( PyObject *row ) {
  char **output;
  int i, nelem = PyTuple_Size(row);

  
  if( (output = (char**) calloc(nelem, sizeof(char*))) == NULL ) {
    return NULL;
  }
  for( i=0; i < nelem; i++ ) {
    if( (output[i] = PyUnicode_AsUTF8(PyTuple_GetItem(row, i))) == NULL ) {
      return NULL;
    }
  }
  return output;
}

static const BlockLocation * 
BlockLocation_Set(PyObject *input,  BlockLocation *output) {
  enum {corrupt, numOfNodes, hosts, names, topologyPaths, length, offset};
  ssize_t i;
  PyObject *item;

  output->corrupt = PyLong_AsLong(PyTuple_GetItem(input, corrupt));
  output->numOfNodes = PyLong_AsLong(PyTuple_GetItem(input, numOfNodes));
  output->hosts = BlockLocation_GetRow(PyTuple_GetItem(input, hosts));
  output->names = BlockLocation_GetRow(PyTuple_GetItem(input, names));
  output->topologyPaths = BlockLocation_GetRow(PyTuple_GetItem(input, topologyPaths));
  output->length = PyLong_AsLong(PyTuple_GetItem(input, length));
  output->offset = PyLong_AsLong(PyTuple_GetItem(input, offset));
  
  return output;
}

static const char getFileBlockLocations_doc[] = 
  "Get an array containing hostnames, offset and size "
  "of portions of the given file";
static PyObject *
getFileBlockLocations(PyObject *self, PyObject *args) {
  hdfsFS fs;
  const char *name;
  tOffset start;
  tOffset i, len;
  BlockLocation *pblocks;
  int nblocks;
  if( !PyArg_ParseTuple(args, "O&s", extract_fs, &fs, &name, &start, &len) ) {
    return NULL;
  }
  if( (pblocks = hdfsGetFileBlockLocations(fs, name, 
					   start, len, &nblocks)) == NULL ) {
    return  hdfs_err();
  }
  
  PyObject *output;
  if( (output = PyTuple_New(nblocks)) == NULL ) {
    return NULL;
  }

  for( i=0; i < nblocks; i++ ) {
    PyObject *elem;
    if( (elem = BlockLocation_New(pblocks + i)) == NULL ) {
      PyObject *type = PyErr_NewException("hdfs3py.error", NULL, NULL);
      PyErr_SetString(type, "could not create BlockLocation");
      return NULL;
    }

    if( 0 != PyTuple_SetItem(output, i, elem) ) {
      return NULL;
    }
  }
  return output;
}

static const char freeFileBlockLocations_doc[] = 
  "Free the BlockLocation array returned by hdfsGetFileBlockLocations";
static PyObject *
freeFileBlockLocations(PyObject *self, PyObject *args) {
  BlockLocation *blocks;
  int i, nblocks;
  PyObject *pynodes;
  if( !PyArg_ParseTuple(args, "O!", &PyTuple_Type, &pynodes) ) {
    return NULL;
  }
  nblocks = PyTuple_Size(pynodes);

  if( (blocks = (BlockLocation*) calloc(nblocks, sizeof(BlockLocation))) == NULL ) {
    return PyErr_NoMemory();
  }
  
  for( i=0; i < nblocks; i++ ) {
    PyObject *elem;
    if( (elem = PyTuple_GetItem(pynodes, i)) == NULL ) {
      return NULL;
    }    
    BlockLocation_Set(elem, blocks + i);
  }

  hdfsFreeFileBlockLocations(blocks, nblocks);

  free(blocks);
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
  { "delete", (PyCFunction)hdfs_delete, METH_VARARGS, PyDoc_STR(delete_doc) }, 
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

static struct PyModuleDef module = {
  PyModuleDef_HEAD_INIT,
  module_name,
  NULL,         /* module documentation, may be NULL */
  -1,           /* size of per-interpreter state of the module,
                   or -1 if the module keeps state in global variables. */
  methods
};

PyMODINIT_FUNC
PyInit_hdfs3py(void)
{
  printf("%ld methods in %s\n", sizeof(methods)/sizeof(*methods), module_name);
  return PyModule_Create(&module);
}
