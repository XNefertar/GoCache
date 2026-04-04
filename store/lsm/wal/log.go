package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

const (
	TypeSet byte = 0 // 插入
	TypeDel byte = 1 // 删除
)

type LogEntry struct {
	Type  byte
	Key   string
	Value []byte
}

type WAL struct {
	mu   sync.Mutex
	file *os.File
	buf  *bufio.Writer
}

func NewWAL(path string) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{
		file: file,
		buf:  bufio.NewWriterSize(file, 64*1024),
	}, nil
}

func (w *WAL) Write(key string, value []byte, isDelete bool) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	var opType byte = TypeSet
	if isDelete {
		opType = TypeDel
	}

	// 自定义二进制序列协议
	// | CRC (4B) | KeyLen (2B) | ValueLen (4B) | Type (1B) | Key (var) | Value (var) |
	keyBuf := []byte(key)
	// 1B Type + 2B KeyLen + 4B ValueLen
	entryHeader := make([]byte, 7)
	entryHeader[0] = opType
	binary.LittleEndian.PutUint16(entryHeader[1:3], uint16(len(keyBuf)))
	binary.LittleEndian.PutUint32(entryHeader[3:7], uint32(len(value)))

	// 计算 CRC 校验码
	crc := crc32.NewIEEE()
	crc.Write(entryHeader)
	crc.Write(keyBuf)
	crc.Write(value)
	checksum := crc.Sum32()

	if err := binary.Write(w.buf, binary.LittleEndian, checksum); err != nil {
		return err
	}
	if _, err := w.buf.Write(entryHeader); err != nil {
		return err
	}
	if _, err := w.buf.Write(keyBuf); err != nil {
		return err
	}
	if _, err := w.buf.Write(value); err != nil {
		return err
	}

	return w.buf.Flush()
}

// Load 崩溃恢复：读取 WAL 文件并重放到内存
func (w *WAL) Load(fn func(entry LogEntry) error) error {
	w.mu.Lock()
	defer w.mu.Lock()
	if _, err := w.file.Seek(0, 0); err != nil {
		return err
	}
	reader := bufio.NewReader(w.file)

	for {
		var savedChecksum uint32
		if err := binary.Read(reader, binary.LittleEndian, &savedChecksum); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		header := make([]byte, 7)
		if _, err := io.ReadFull(reader, header); err != nil {
			return err
		}
		opType := header[0]
		keyLen := binary.LittleEndian.Uint16(header[1:3])
		valLen := binary.LittleEndian.Uint32(header[3:7])

		keyBuf := make([]byte, keyLen)
		if _, err := io.ReadFull(reader, keyBuf); err != nil {
			return nil
		}
		valBuf := make([]byte, valLen)
		if _, err := io.ReadFull(reader, valBuf); err != nil {
			return err
		}

		crc := crc32.NewIEEE()
		crc.Write(header)
		crc.Write(keyBuf)
		crc.Write(valBuf)
		if crc.Sum32() != savedChecksum {
			return fmt.Errorf("WAL corruption detected: checksum mismatch")
		}

		err := fn(LogEntry{Type: opType, Key: string(keyBuf), Value: valBuf})
		if err != nil {
			return nil
		}
	}
	return nil
}
