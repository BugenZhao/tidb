// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package backup

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/pingcap/parser/model"
	"github.com/zeebo/blake3"
)

const (
	maskDatabasePrefix = "_mdb"
	maskTablePrefix    = "_mtable"
	maskColumnPrefix   = "_mcol"
	defaultHashContext = "tidb"
)

func newColumnDictionary() *dictionary {
	return newDictionary(defaultHashContext, maskColumnPrefix)
}

func newDictionary(context string, prefix string) *dictionary {
	return &dictionary{
		hasher: blake3.NewDeriveKey(context),
		prefix: prefix,
		dict:   make(map[string]uint32),
		values: make(map[uint32]bool),
	}
}

type dictionary struct {
	hasher *blake3.Hasher
	prefix string

	dict   map[string]uint32
	values map[uint32]bool
}

func (d *dictionary) get(key string) string {
	if value, ok := d.dict[key]; ok {
		return fmt.Sprintf("%s%s", d.prefix, strconv.FormatUint(uint64(value), 36))
	}
	return ""
}

func (d *dictionary) Map(key string) string {
	value := d.get(key)
	if value != "" {
		return value
	}

	d.hasher.Reset()
	_, _ = d.hasher.Write([]byte(key))
	sum := make([]byte, 4)
	_, err := d.hasher.Digest().Read(sum)
	if err != nil {
		panic(err)
	}
	u := binary.LittleEndian.Uint32(sum)

	for {
		if d.values[u] {
			u += 1
			continue
		}
		d.values[u] = true
		d.dict[key] = u
		return d.get(key)
	}
}

func (ss *Schemas) MaskSchemasNames() {
	dbName := make([]nameID, 0)
	tableName := make([]nameID, 0)
	mark := make(map[int64]bool)
	for _, s := range ss.schemas {
		db := s.dbInfo.Name.O
		if _, ok := mark[s.dbInfo.ID]; !ok {
			dbName = append(dbName, nameID{name: db, id: s.dbInfo.ID})
			mark[s.dbInfo.ID] = true
		}
		tbl := fmt.Sprintf("%s.%s", db, s.tableInfo.Name.O)
		if _, ok := mark[s.tableInfo.ID]; !ok {
			tableName = append(tableName, nameID{name: tbl, id: s.tableInfo.ID})
			mark[s.tableInfo.ID] = true
		}
		for _, t := range s.dbInfo.Tables {
			tbl := fmt.Sprintf("%s.%s", db, t.Name.O)
			if _, ok := mark[t.ID]; !ok {
				tableName = append(tableName, nameID{name: tbl, id: t.ID})
				mark[t.ID] = true
			}
		}
	}
	dbMap := sortedNameIDToMap(maskDatabasePrefix, dbName)
	tableMap := sortedNameIDToMap(maskTablePrefix, tableName)
	colDict := newColumnDictionary()
	for _, s := range ss.schemas {
		for _, t := range s.dbInfo.Tables {
			renameTable(tableMap, colDict, t)
		}
		renameTable(tableMap, colDict, s.tableInfo)
		if n, ok := dbMap[s.dbInfo.ID]; ok {
			rename(&s.dbInfo.Name, n)
		}
	}
}

func renameTable(tableMap map[int64]string, dict *dictionary, table *model.TableInfo) {
	n, ok := tableMap[table.ID]
	if !ok || table.Name.O == n {
		// this table reference has already renamed
		return
	}
	rename(&table.Name, n)
	for _, col := range table.Columns {
		colName := dict.Map(col.Name.L)
		rename(&col.Name, colName)
		// Mask the enum elements
		if len(col.Elems) != 0 {
			newElems := make([]string, 0)
			for _, e := range col.Elems {
				newElems = append(newElems, maskString([]byte(e)))
			}
			col.Elems = newElems
		}
	}
	// rename index
	for _, idx := range table.Indices {
		rename(&idx.Table, n)
		for _, c := range idx.Columns {
			rename(&c.Name, dict.Map(c.Name.L))
		}
	}
	// rename constraint
	for _, constraint := range table.Constraints {
		rename(&constraint.Table, n)
		for i, c := range constraint.ConstraintCols {
			rename(&constraint.ConstraintCols[i], dict.Map(c.L))
		}
	}
	// rename partition
	if table.Partition != nil {
		for i, c := range table.Partition.Columns {
			rename(&table.Partition.Columns[i], dict.Map(c.L))
		}
	}
	// clear foreign key
	table.ForeignKeys = make([]*model.FKInfo, 0)

}

func rename(n *model.CIStr, newName string) {
	*n = model.NewCIStr(newName)
}

func sortedNameIDToMap(prefix string, names []nameID) map[int64]string {
	sort.Slice(names[:], func(l, r int) bool {
		return names[l].name < names[r].name
	})
	padingLen := len(strconv.Itoa(len(names)))
	nameMap := make(map[int64]string)
	for i, n := range names {
		nameMap[n.id] = fmt.Sprintf("%s%0*d", prefix, padingLen, i)
	}
	return nameMap
}

func sortedStringToMap(prefix string, names []string) map[string]string {
	sort.Strings(names)
	padingLen := len(strconv.Itoa(len(names)))
	nameMap := make(map[string]string)
	for i, n := range names {
		nameMap[n] = fmt.Sprintf("%s%0*d", prefix, padingLen, i)
	}
	return nameMap
}

func hashBytes(data interface{}, size int) []byte {
	var bs []byte
	switch data := data.(type) {
	case []byte:
		bs = data
	default:
		buf := new(bytes.Buffer)
		_ = binary.Write(buf, binary.LittleEndian, data)
		bs = buf.Bytes()
	}

	hasher := blake3.NewDeriveKey(defaultHashContext)
	_, _ = hasher.Write(bs)

	sum := make([]byte, size)
	n, err := hasher.Digest().Read(sum)
	if err != nil {
		panic(err)
	}
	if n != size {
		panic(fmt.Sprintf("bad size `%d` vs `%d`", n, size))
	}

	return sum
}

func maskString(s []byte) string {
	size := len(s)

	sum := hashBytes([]byte(s), size/2)
	hex := hex.EncodeToString(sum)
	hex = hex + strings.Repeat("*", size-len(hex))
	return hex
}
