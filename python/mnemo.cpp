#include "mnemo.h"

using namespace std;

static mnemo::handles_t <void *, size_t> pointers;

/*
 * Keep a pointer, return an index. 
 */
size_t mn_put( void *v ) {
  return pointers.put(v);
}
  
/*
 * Accept an index, return a pointer. 
 */
void * mn_get( size_t pos ) {
  return pointers.get(pos);
}

/*
 * Accept an index, return a pointer and forget it. 
 */
void * mn_del( size_t pos ) {
  return pointers.del(pos);
}
