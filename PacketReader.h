#ifndef __AMPACKETREADER_H__
#define __AMPACKETREADER_H__

#include "amdefs.h"

class PacketReader
{
private:
	char *m_buffStart;
	char *m_buffEnd;
	char *m_readCursor;
	char *m_writeCursor;
	char *m_packetEnd;

public:

	PacketReader (size_t cbSize);
	~PacketReader (void);
	void skip();
	void push(size_t _cbData);
	char *getWritePtr();
	char *getStartPtr();
	char *getEndPtr();
	size_t getSize();
	bool havePacket();
	
	UINT8 readByte();
	UINT16 readShort();
	UINT32 readINT24();
	UINT32 readLong();
	char * readNTString();
	UINT8 *readBytes(size_t cbsize);
	size_t getBytesLeft();
	void rewind(size_t num);
	UINT64 readLengthCodedInteger();
	UINT8 *readLengthCodedBinary(size_t *_outLen);
};

#endif