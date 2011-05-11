#ifndef __AMPACKETWRITER_H__
#define __AMPACKETWRITER_H__

#include "amdefs.h"
#include "socketdefs.h"

class PacketWriter
{
public:
	PacketWriter(size_t _cbSize);
	~PacketWriter(void);

	// Push/increment write cursor
	void push(void *data, size_t cbData);

	// Pull/Increment read cursor
	void pull(size_t cbSize);

	char *getStart();
	char *getEnd();
	char *getReadCursor();
	char *getWriteCursor();
	bool isDone();
	void reset();

	void writeLong (UINT32 value);
	void writeByte (UINT8 value);
	void writeNTString (const char *_str);
	void writeBytes (void *data, size_t cbData);
	void finalize(int packetNumber);

	size_t getSize(void);


private:
	char *m_buffStart;
	char *m_buffEnd;
	char *m_readCursor;
	char *m_writeCursor;


};

#endif