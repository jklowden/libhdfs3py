#ifndef _MNEMO_H_
#define _MNEMO_H_

#ifdef __cplusplus

#include <algorithm>
#include <set>
#include <vector>

#include <cassert>

namespace mnemo {
  
  template <typename V, typename I>
  class handles_t { 
  private: 
    std::vector<V> handles;
    std::set<size_t> unused;
    size_t nelem = 0;
  public:
    const V INVALID;

    handles_t() : handles(16),  INVALID(reinterpret_cast<V>(-1)) {}
    /*
     * Keep a value, return an index. 
     */
    I put( V v ) {
      // Don't store the same value twice. 
      typename std::vector<V>::iterator pv;
      I i(0);
      pv = std::find_if( handles.begin(), handles.end(), [this, v, &i](V) {
	  return unused.find(i++) != unused.end();
	} );
      if( pv != handles.end() ) {
	// We found a value in handles whose index is not in unused. 
	return static_cast<I>(-1);
      }
   
      // If there's an available spot in the cache, use it. 
      typename std::set<I>::iterator p;
      if( (p = unused.begin()) != unused.end() ) {
	const I pos = *p;
	handles.at(pos) = v;
	unused.erase(p);
	return pos;
      }
    
      if( nelem == handles.size() ) {
	handles.resize( 2 * handles.size() );
      }

      handles.at(nelem) = v;
    
      return nelem++;
    }

    /*
     * Accept an index, return a value. 
     */
    V get( I pos ) const {
      assert(nelem < handles.size());
      if( pos >= nelem ) {
	return INVALID;
      }

      return handles.at(pos);
    }

    /*
     * Accept an index, return a value and forget it. 
     */
    V del( I pos ) {
      assert(nelem < handles.size());
      if( pos >= nelem ) {
	return INVALID;
      }

      V v( handles.at(pos) );

      unused.insert(pos);

      return v;
    }

  };
}  //namespace

/*
 * Public C functions
 */

extern "C" {
#endif

  size_t mn_put( const void *v );
  void * mn_get( size_t pos );
  void * mn_del( size_t pos );

#ifdef __cplusplus
}
#endif

#endif /* _MNEMO_H_ */
