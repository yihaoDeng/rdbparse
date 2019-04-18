#ifndef __INTSET_H__
#define __INTSET_H__

#include "slash/include/env.h"

using namespace slash;
struct Intset {
   uint32_t encoding; 
   uint32_t length;
   int8_t content[0];
   Status Get(size_t pos, int64_t *v);
   Status Dump();
};
#endif
