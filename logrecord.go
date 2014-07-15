package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/deafbybeheading/femebe/buf"
)

type logRecord struct {
	LogTime          string
	UserName         *string
	DatabaseName     *string
	Pid              int32
	ClientAddr       *string
	SessionId        string
	SeqNum           int64
	PsDisplay        *string
	SessionStart     string
	Vxid             *string
	Txid             uint64
	ELevel           int32
	SQLState         *string
	ErrMessage       *string
	ErrDetail        *string
	ErrHint          *string
	InternalQuery    *string
	InternalQueryPos int32
	ErrContext       *string
	UserQuery        *string
	UserQueryPos     int32
	FileErrPos       *string
	ApplicationName  *string
}

func (lr *logRecord) oneLine() []byte {
	buf := bytes.Buffer{}

	wd := func() {
		buf.WriteByte(' ')
	}

	ws := func(name string, s string) {
		buf.WriteString(fmt.Sprintf("%s=%q", name, s))
	}

	wns := func(name string, s *string) {
		body := func() string {
			if s == nil {
				return "NULL"
			}

			return fmt.Sprintf("[%q]", *s)
		}()

		buf.WriteString(name)
		buf.WriteByte('=')
		buf.WriteString(body)
	}

	wnum := func(name string, n interface{}) {
		buf.WriteString(fmt.Sprintf("%s=%d", name, n))
	}

	ws("LogTime", lr.LogTime)
	wd()
	wns("UserName", lr.UserName)
	wd()
	wns("DatabaseName", lr.DatabaseName)
	wd()
	wnum("Pid", lr.Pid)
	wd()
	wns("ClientAddr", lr.ClientAddr)
	wd()
	ws("SessionId", lr.SessionId)
	wd()
	wnum("SeqNum", lr.SeqNum)
	wd()
	wns("PsDisplay", lr.PsDisplay)
	wd()
	ws("SessionStart", lr.SessionStart)
	wd()
	wns("Vxid", lr.Vxid)
	wd()
	wnum("Txid", lr.Txid)
	wd()
	wnum("ELevel", lr.ELevel)
	wd()
	wns("SQLState", lr.SQLState)
	wd()
	wns("ErrMessage", lr.ErrMessage)
	wd()
	wns("ErrDetail", lr.ErrDetail)
	wd()
	wns("ErrHint", lr.ErrHint)
	wd()
	wns("InternalQuery", lr.InternalQuery)
	wd()
	wnum("InternalQueryPos", lr.InternalQueryPos)
	wd()
	wns("ErrContext", lr.ErrContext)
	wd()
	wns("UserQuery", lr.UserQuery)
	wd()
	wnum("UserQueryPos", lr.UserQueryPos)
	wd()
	wns("FileErrPos", lr.FileErrPos)
	wd()
	wns("ApplicationName", lr.ApplicationName)

	return buf.Bytes()
}

func readInt64(r io.Reader) (ret int64, err error) {
	var be [8]byte

	valBytes := be[0:8]
	if _, err = io.ReadFull(r, valBytes); err != nil {
		return 0, err
	}

	return int64(binary.BigEndian.Uint64(valBytes)), nil
}

func readUint64(r io.Reader) (ret uint64, err error) {
	var be [8]byte

	valBytes := be[0:8]
	if _, err = io.ReadFull(r, valBytes); err != nil {
		return 0, err
	}

	return binary.BigEndian.Uint64(valBytes), nil
}

func parseLogRecord(
	dst *logRecord, data []byte, exit exitFn) {

	b := bytes.NewBuffer(data)

	// Read the next nullable string from b, returning a 'nil'
	// *string should it be null.
	nextNullableString := func() *string {
		np, err := buf.ReadByte(b)
		if err != nil {
			exit(err)
		}

		switch np {
		case 'P':
			s, err := buf.ReadCString(b)
			if err != nil {
				exit(err)
			}

			return &s

		case 'N':
			// 'N' is still followed by a NUL byte that
			// must be consumed.
			_, err := buf.ReadCString(b)
			if err != nil {
				exit(err)
			}

			return nil

		default:
			exit("Expected nullable string "+
				"control character, got %c", np)

		}

		exit("Prior switch should always return")
		panic("exit should panic/return, " +
			"but the compiler doesn't know that")
	}

	// Read a non-nullable string from b
	nextString := func() string {
		s, err := buf.ReadCString(b)
		if err != nil {
			exit(err)
		}

		return s
	}

	nextInt32 := func() int32 {
		i32, err := buf.ReadInt32(b)
		if err != nil {
			exit(err)
		}

		return i32
	}

	nextInt64 := func() int64 {
		i64, err := readInt64(b)
		if err != nil {
			exit(err)
		}

		return i64
	}

	nextUint64 := func() uint64 {
		ui64, err := readUint64(b)
		if err != nil {
			exit(err)
		}

		return ui64
	}

	dst.LogTime = nextString()
	dst.UserName = nextNullableString()
	dst.DatabaseName = nextNullableString()
	dst.Pid = nextInt32()
	dst.ClientAddr = nextNullableString()
	dst.SessionId = nextString()
	dst.SeqNum = nextInt64()
	dst.PsDisplay = nextNullableString()
	dst.SessionStart = nextString()
	dst.Vxid = nextNullableString()
	dst.Txid = nextUint64()
	dst.ELevel = nextInt32()
	dst.SQLState = nextNullableString()
	dst.ErrMessage = nextNullableString()
	dst.ErrDetail = nextNullableString()
	dst.ErrHint = nextNullableString()
	dst.InternalQuery = nextNullableString()
	dst.InternalQueryPos = nextInt32()
	dst.ErrContext = nextNullableString()
	dst.UserQuery = nextNullableString()
	dst.UserQueryPos = nextInt32()
	dst.FileErrPos = nextNullableString()
	dst.ApplicationName = nextNullableString()

	if b.Len() != 0 {
		exit("LogRecord message has mismatched "+
			"length header and cString contents: remaining %d",
			b.Len())
	}
}
