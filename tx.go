package main

import (
	"encoding/binary"
	"github.com/boltdb/bolt"
	"io"
	"bytes"
	"errors"
	"syscall"
	"time"
)

type TxOp byte
type TxNameLen uint16

const ( // dont reorder these.  storage format.
	TX_MKDIR TxOp = iota
	TX_REMOVE
	TX_RENAME
)

type Tx struct {
	Version uint16

	Unix uint64
	Dbid uint16
	Txid uint64

	Op	TxOp

	Inode uint64
	Name []byte
	Inode2 uint64
	Name2 []byte
}


// serialize to bolt storage
func (txn *Tx) ToKV() ([]byte, []byte, error) {
	kbody := bytes.Buffer{}
	body := bytes.Buffer{}

	var err error

	l1 := TxNameLen(len(txn.Name))
	l2 := TxNameLen(len(txn.Name2))

	err = binary.Write(&kbody, binary.LittleEndian, txn.Version)          ; if(err != nil) { return nil, nil, err }
	err = binary.Write(&kbody, binary.LittleEndian, txn.Unix)             ; if(err != nil) { return nil, nil, err }
	err = binary.Write(&kbody, binary.LittleEndian, txn.Dbid)             ; if(err != nil) { return nil, nil, err }
	err = binary.Write(&kbody, binary.LittleEndian, txn.Txid)             ; if(err != nil) { return nil, nil, err }

	err = binary.Write(&body, binary.LittleEndian, txn.Op)                 ; if(err != nil) { return nil, nil, err }
	err = binary.Write(&body, binary.LittleEndian, txn.Inode)              ; if(err != nil) { return nil, nil, err }
	err = binary.Write(&body, binary.LittleEndian, l1)                     ; if(err != nil) { return nil, nil, err }
	err = binary.Write(&body, binary.LittleEndian, txn.Inode2)             ; if(err != nil) { return nil, nil, err }
	err = binary.Write(&body, binary.LittleEndian, l2)                     ; if(err != nil) { return nil, nil, err }

	return kbody.Bytes(), body.Bytes(), nil
}

// deserialize from bolt storage
func TxFromKV(k, v []byte) (*Tx, error) {
	kbody := bytes.NewBuffer(k)
	body := bytes.NewBuffer(v)

	txn := Tx{}

	var err error

	err = binary.Read(kbody, binary.LittleEndian, &txn.Version) ; if(err != nil) { return nil, err}
	if txn.Version != 1 {
		return nil, errors.New("Unsupported transaction version")
	}
	err = binary.Read(kbody, binary.LittleEndian, &txn.Unix) ; if(err != nil) { return nil, err}
	err = binary.Read(kbody, binary.LittleEndian, &txn.Dbid) ; if(err != nil) { return nil, err}
	err = binary.Read(kbody, binary.LittleEndian, &txn.Txid) ; if(err != nil) { return nil, err}

	err = binary.Read(body, binary.LittleEndian, &txn.Op) ; if(err != nil) { return nil, err}
	err = binary.Read(body, binary.LittleEndian, &txn.Inode) ; if(err != nil) { return nil, err}
	var l TxNameLen
	err = binary.Read(body, binary.LittleEndian, &l) ; if(err != nil) { return nil, err}
	if l > 0 {
		txn.Name = make([]byte, l)
		err = binary.Read(body, binary.LittleEndian, &txn.Name) ; if(err != nil) { return nil, err}
	}
	err = binary.Read(body, binary.LittleEndian, &txn.Inode2) ; if(err != nil) { return nil, err}
	err = binary.Read(body, binary.LittleEndian, &l) ; if(err != nil) { return nil, err}
	if l > 0 {
		txn.Name2 = make([]byte, l)
		err = binary.Read(body, binary.LittleEndian, &txn.Name2) ; if(err != nil) { return nil, err}
	}
	return &txn, nil
}

// serialize to a socket
func (txn *Tx) WriteTo(p io.Writer) error {
	var err error

	err = binary.Write(p, binary.LittleEndian, txn.Version)           ; if(err != nil) { return err }
	err = binary.Write(p, binary.LittleEndian, txn.Unix)              ; if(err != nil) { return err }
	err = binary.Write(p, binary.LittleEndian, txn.Dbid)              ; if(err != nil) { return err }
	err = binary.Write(p, binary.LittleEndian, txn.Txid)              ; if(err != nil) { return err }

	l1 := TxNameLen(len(txn.Name))
	l2 := TxNameLen(len(txn.Name2))

	err = binary.Write(p, binary.LittleEndian, txn.Op)                 ; if(err != nil) { return err }
	err = binary.Write(p, binary.LittleEndian, txn.Inode)              ; if(err != nil) { return err }
	err = binary.Write(p, binary.LittleEndian, l1)                     ; if(err != nil) { return err }
	err = binary.Write(p, binary.LittleEndian, txn.Inode2)             ; if(err != nil) { return err }
	err = binary.Write(p, binary.LittleEndian, l2)                     ; if(err != nil) { return err }

	return nil
}

// deserialize from a socket
func TxReadFrom(p io.Reader) (*Tx, error) {
	var err error

	txn := Tx{}

	err = binary.Read(p, binary.LittleEndian, &txn.Version) ; if err != nil { return nil, err }
	if txn.Version != 1 {
		return nil, errors.New("Unsupported transaction version")
	}
	err = binary.Read(p, binary.LittleEndian, &txn.Unix) ; if err != nil { return nil, err }
	err = binary.Read(p, binary.LittleEndian, &txn.Dbid) ; if err != nil { return nil, err }
	err = binary.Read(p, binary.LittleEndian, &txn.Txid) ; if err != nil { return nil, err }

	var l TxNameLen

	err = binary.Read(p, binary.LittleEndian, &txn.Op) ; if err != nil { return nil, err }
	err = binary.Read(p, binary.LittleEndian, &txn.Inode) ; if err != nil { return nil, err }
	err = binary.Read(p, binary.LittleEndian, &l) ; if(err != nil) { return nil, err }
	if l > 0 {
		txn.Name = make([]byte, l)
		err = binary.Read(p, binary.LittleEndian, &txn.Name) ; if(err != nil) { return nil, err}
	}
	err = binary.Read(p, binary.LittleEndian, &txn.Inode2) ; if(err != nil) { return nil, err}
	err = binary.Read(p, binary.LittleEndian, &l) ; if(err != nil) { return nil, err}
	if l > 0 {
		txn.Name2 = make([]byte, l)
		err = binary.Read(p, binary.LittleEndian, &txn.Name2) ; if(err != nil) { return nil, err}
	}
	return &txn, nil
}

func (f *FS) NewTx(tx *bolt.Tx, op TxOp, Inode uint64, Name []byte, Inode2 uint64, Name2 []byte) (*Tx, error) {
	if len(Name) > max_name_len || len(Name2) > max_name_len {
		return nil, syscall.ENAMETOOLONG
	}

	ts := time.Now().Unix()
	if ts < 0 {
		return nil, errors.New("Are you in the past?")
	}

	id, err := f.NewTxId(tx)
	if err != nil {
		return nil, err
	}

	txn := Tx{
		Version: 1,

		Unix: uint64(ts),
		Dbid: f.dbid,
		Txid: id,

		Op: op,
		Inode: Inode,
		Name: Name,
		Inode2: Inode2,
		Name2: Name2,
	}

	return &txn, nil
}



/* OPS

TX_MKDIR
Inode: parent dir
Name: name of new folder
Inode2: new folder inode

TX_REMOVE
Inode: parent dir
Name: path to remove

TX_RENAME
Inode: parent dir
Name: filename to move from
Inode2: new parent dir
Name2: filename to move to

*/


/*
CONFLICT RESOLUTION

creation/rename conflict (file or dir):
	client will insert an ALIAS for the remote inode,
	something like name-<dbid>.  log replay will use
	alias index to correct transactions on that folder.
	transaction logs will ALWAYS show the original path,
	even if it conflicts.  correction via aliases will
	be local to the database's own view/storage.

	to correct a conflicting filename (so when you see an aliased version in your directory):
	move your file/dir to a new name.  the alias should immediately disappear, and the other version
	of the file will take its real pathname.  (this will occur due to immediate transaction replay
	after your mv).

	you may then rename the path, remove it, or put your folders contents into it and then remove
	yours.

remove conflict:
	client A removes a path, after which client B does something to it before syncing.

	when client B syncs, it will set a ZOMBIE flag for that path.  ZOMBIE flagged paths
	will only be visible to the dbid that modified the path after deletion (might not be
	the original creator of the path).

	client B will see their folder/file rename to path-ZOMBIE.  from here they can rename
	it to restore it, or remove it for permanent removal.


*/