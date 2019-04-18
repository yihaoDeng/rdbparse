#ifndef __UTIL_H__
#define __UTIL_H__

void memrev16(void *p);
void memrev32(void *p);
void memrev64(void *p);

uint64_t crc64(uint64_t crc, const unsigned char *s, uint64_t l);
/* variants of the function doing the actual convertion only if the target
 *  * host is big endian */
#if (BYTE_ORDER == LITTLE_ENDIAN)
#define memrev16ifbe(p)
#define memrev32ifbe(p)
#define memrev64ifbe(p)
#else
#define memrev16ifbe(p) memrev16(p)
#define memrev32ifbe(p) memrev32(p)
#define memrev64ifbe(p) memrev64(p)
#endif

#endif

