package main

import (
	"github.com/boltdb/bolt"
	"fmt"
	"os"
	"bytes"
	"encoding/binary"
	"encoding/hex"
)

func b_uint64(b []byte) uint64 {
	var i uint64
	buf := bytes.NewReader(b)
	binary.Read(buf, binary.LittleEndian, &i)
	return i
}

func bfmt(b []byte) string {
	for _, v := range b {
		if v < 32 || v > 126 {
			if(len(b) == 8) {
				return fmt.Sprintf("%d", b_uint64(b))
			} else {
				return hex.EncodeToString(b)
			}
		}
	}
	return fmt.Sprintf("\"%s\"", b)
}

func indent(i int) {
	for i > 0 {
		fmt.Printf("   ")
		i--
	}
}
func recursor(b *bolt.Bucket, i int) error {
	c := b.Cursor()

	for k, v := c.First(); k != nil; k, v = c.Next() {
		indent(i)

		if v == nil {
			fmt.Println(bfmt(k))
			err := recursor(b.Bucket(k), i + 1)
			if err != nil {
				return err
			}
		} else {
			fmt.Println(bfmt(k) + ": " + bfmt(v))
		}
	}
	return nil
}

func main() {
	if len(os.Args) != 2 {
		fmt.Println("pass bolt database filename")
		return
	}

	filename := os.Args[1]

	_, err := os.Stat(filename)
	if err != nil {
	    fmt.Println(err)
		 return
	}

	db, err := bolt.Open(filename, 0600, nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer db.Close()

	err = db.View(func(tx *bolt.Tx) error {
		c := tx.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			fmt.Println(bfmt(k))
			if v != nil {
				panic("wtf")
			}
			err := recursor(tx.Bucket(k), 1)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		fmt.Println(err)
		return
	}
}
