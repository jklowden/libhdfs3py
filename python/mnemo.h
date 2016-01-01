#ifndef _MNEMO_H_
#define _MNEMO_H_

#ifdef __cplusplus

#include <algorithm>
#include <set>
#include <vector>

#include <cassert>

namespace mnemo {
  
  template <typename V, typename I = size_t>
  class handles_t { 
  private: 
    std::vector<V> handles;
    std::set<I> unused;
    size_t nelem = 0;
  protected:
    typedef typename std::vector<V>::iterator iterator_v;
    typedef typename std::vector<V>::const_iterator iterator_cv;
    iterator_cv find_value(V value) const {
      I i(0);
      return 
	std::find_if( handles.begin(), handles.end(), [this, value, &i](V v) {
	    return v == value && unused.find(i++) != unused.end();
	  } );
    }
  public:
    const V INVALID;
    handles_t() : handles(16),  INVALID(reinterpret_cast<V>(-1)) {}

    /*
     * Keep a value, return an index. 
     */
    I put( V v ) {
      // Don't store the same value twice. 
      iterator_cv pv;
      if( (pv = find_value(v)) != handles.end() ) {
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
     * Verify a value. 
     */
    bool vet( V v ) const {
      return find_value(v) != handles.end();
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

    /*
     * Forget a value. 
     */
    bool del( V v ) {
      iterator_v pv = find_value(v);
      if( pv == handles.end() )  {
	return false;
      }
      const size_t pos = pv - handles.begin();
      unused.insert(pos);
      return true;
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
