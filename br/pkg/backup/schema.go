// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package backup

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/br/pkg/checksum"
	"github.com/pingcap/tidb/br/pkg/glue"
	brkv "github.com/pingcap/tidb/br/pkg/kv"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/kv"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/zeebo/blake3"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	// DefaultSchemaConcurrency is the default number of the concurrent
	// backup schema tasks.
	DefaultSchemaConcurrency = 64
	CreateDBFileSuffix       = "schema-create.sql"
	CreateTableFileSuffix    = "schema.sql"
	MaskDatabasePrefix       = "MASKED_DB"
	MaskTablePrefix          = "TABLE"
	MaskColumnPrefix         = "COL"
	defaultContext           = "tidb"
)

func GetCommonHandleId(t model.TableInfo) []int64 {
	if !t.IsCommonHandle {
		return nil
	}
	var ids []int64
	for _, i := range t.Indices {
		if i.Primary {
			// Iterate the `indexColumns` fist, so the order in `indexColumns` can preserve
			for _, iCol := range i.Columns {
				for _, tCol := range t.Columns {
					if tCol.Name == iCol.Name {
						ids = append(ids, t.ID)
						break
					}
				}
			}
			return ids
		}
	}
	return nil
}

func TableToProto(ctx sessionctx.Context, name string, t model.TableInfo) (*tipb.TableInfo, error) {
	ch := GetCommonHandleId(t)
	cols := util.ColumnsToProto(t.Columns, t.PKIsHandle)
	// Set the default value of the column
	err := plannercore.SetPBColumnsDefaultValue(ctx, cols, t.Columns)
	return &tipb.TableInfo{
		Name:          &name,
		TableId:       t.ID,
		Columns:       cols,
		CommonHandles: ch,
	}, err
}

type scheamInfo struct {
	tableInfo  *model.TableInfo
	dbInfo     *model.DBInfo
	crc64xor   uint64
	totalKvs   uint64
	totalBytes uint64
	stats      *handle.JSONTable
}

// Schemas is task for backuping schemas.
type Schemas struct {
	// name -> schema
	schemas map[string]*scheamInfo
}

func newBackupSchemas() *Schemas {
	return &Schemas{
		schemas: make(map[string]*scheamInfo),
	}
}

func (ss *Schemas) addSchema(
	dbInfo *model.DBInfo, tableInfo *model.TableInfo,
) {
	name := fmt.Sprintf("%s.%s",
		utils.EncloseName(dbInfo.Name.L), utils.EncloseName(tableInfo.Name.L))
	ss.schemas[name] = &scheamInfo{
		tableInfo: tableInfo,
		dbInfo:    dbInfo,
	}
}

type nameID struct {
	name string
	id   int64
}

func (ss *Schemas) BackupSchemaInSQL(prefix string, ctx context.Context, g glue.Glue, externalStore storage.ExternalStorage, store kv.Storage) ([]*backuppb.File, error) {
	se, err := g.CreateSession(store)
	if err != nil {
		return nil, errors.Trace(err)
	}
	mark := make(map[string]bool)
	files := make([]*backuppb.File, 0)
	for _, s := range ss.schemas {
		if _, ok := mark[s.dbInfo.Name.O]; !ok {
			str, err := se.ShowCreateDatabase(s.dbInfo)
			if err != nil {
				return nil, errors.Trace(err)
			}
			d := []byte(fmt.Sprintf("%s;\n", str))
			fileName := fmt.Sprintf("%s%s-%s", prefix, s.dbInfo.Name.O, CreateDBFileSuffix)
			files = append(files, &backuppb.File{Name: fileName, Size_: uint64(len(d))})
			err = externalStore.WriteFile(ctx, fileName, d)
			if err != nil {
				return nil, errors.Trace(err)
			}
			mark[s.dbInfo.Name.O] = true
		}
		str, err := se.ShowCreateTable(s.tableInfo)
		if err != nil {
			return nil, errors.Trace(err)
		}
		d := []byte(fmt.Sprintf("%s;\n", str))
		fileName := fmt.Sprintf("%s%s.%s-%s", prefix, s.dbInfo.Name.O, s.tableInfo.Name.O, CreateTableFileSuffix)
		files = append(files, &backuppb.File{Name: fileName, Size_: uint64(len(d))})
		err = externalStore.WriteFile(ctx, fileName, d)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return files, nil
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
	dbMap := sortedNameIDToMap(MaskDatabasePrefix, dbName)
	tableMap := sortedNameIDToMap(MaskTablePrefix, tableName)
	for _, s := range ss.schemas {
		db := s.dbInfo.Name.O
		for _, t := range s.dbInfo.Tables {
			renameTable(tableMap, db, t)
		}
		renameTable(tableMap, db, s.tableInfo)
		if n, ok := dbMap[s.dbInfo.ID]; ok {
			rename(&s.dbInfo.Name, n)
		}
	}
}

func renameTable(tableMap map[int64]string, db string, table *model.TableInfo) {
	n, ok := tableMap[table.ID]
	if !ok || table.Name.O == n {
		// this table reference has already renamed
		return
	}
	rename(&table.Name, n)
	colMap := make(map[string]string)
	for _, col := range table.Columns {
		colName := fmt.Sprintf("%s%d_%d", MaskColumnPrefix, table.ID, col.Offset)
		colMap[col.Name.O] = colName
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
			if colName, ok := colMap[c.Name.O]; ok {
				rename(&c.Name, colName)
			}
		}
	}
	// rename constraint
	for _, constraint := range table.Constraints {
		rename(&constraint.Table, n)
		for i, c := range constraint.ConstraintCols {
			if colName, ok := colMap[c.O]; ok {
				rename(&constraint.ConstraintCols[i], colName)
			}
		}
	}
	// rename partition
	if table.Partition != nil {
		for i, c := range table.Partition.Columns {
			if colName, ok := colMap[c.O]; ok {
				rename(&table.Partition.Columns[i], colName)
			}
		}
	}
	// clear foreign key
	table.ForeignKeys = make([]*model.FKInfo, 0)

}

func rename(n *model.CIStr, newName string) {
	n.O = newName
	n.L = strings.ToLower(newName)
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

	hasher := blake3.NewDeriveKey(defaultContext)
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

// BackupSchemas backups table info, including checksum and stats.
func (ss *Schemas) BackupSchemas(
	ctx context.Context,
	metaWriter *metautil.MetaWriter,
	store kv.Storage,
	statsHandle *handle.Handle,
	backupTS uint64,
	concurrency uint,
	copConcurrency uint,
	skipChecksum bool,
	updateCh glue.Progress,
) error {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("Schemas.BackupSchemas", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	workerPool := utils.NewWorkerPool(concurrency, "Schemas")
	errg, ectx := errgroup.WithContext(ctx)
	startAll := time.Now()
	op := metautil.AppendSchema
	metaWriter.StartWriteMetasAsync(ctx, op)

	sessionCxt := brkv.NewSession(&brkv.SessionOptions{
		Timestamp:        time.Now().Unix(),
		RowFormatVersion: "2",
	})

	for _, s := range ss.schemas {
		schema := s
		workerPool.ApplyOnErrorGroup(errg, func() error {
			if utils.IsSysDB(schema.dbInfo.Name.L) {
				schema.dbInfo.Name = utils.TemporaryDBName(schema.dbInfo.Name.O)
			}
			logger := log.With(
				zap.String("db", schema.dbInfo.Name.O),
				zap.String("table", schema.tableInfo.Name.O),
			)

			if !skipChecksum {
				logger.Info("table checksum start")
				start := time.Now()
				checksumResp, err := calculateChecksum(
					ectx, schema.tableInfo, store.GetClient(), backupTS, copConcurrency)
				if err != nil {
					return errors.Trace(err)
				}
				schema.crc64xor = checksumResp.Checksum
				schema.totalKvs = checksumResp.TotalKvs
				schema.totalBytes = checksumResp.TotalBytes
				logger.Info("table checksum finished",
					zap.Uint64("Crc64Xor", checksumResp.Checksum),
					zap.Uint64("TotalKvs", checksumResp.TotalKvs),
					zap.Uint64("TotalBytes", checksumResp.TotalBytes),
					zap.Duration("take", time.Since(start)))
			}
			if statsHandle != nil {
				jsonTable, err := statsHandle.DumpStatsToJSON(
					schema.dbInfo.Name.String(), schema.tableInfo, nil)
				if err != nil {
					logger.Error("dump table stats failed", logutil.ShortError(err))
				}
				schema.stats = jsonTable
			}
			// Send schema to metawriter
			dbBytes, err := json.Marshal(schema.dbInfo)
			if err != nil {
				return errors.Trace(err)
			}
			tableBytes, err := json.Marshal(schema.tableInfo)
			if err != nil {
				return errors.Trace(err)
			}
			var statsBytes []byte
			if schema.stats != nil {
				statsBytes, err = json.Marshal(schema.stats)
				if err != nil {
					return errors.Trace(err)
				}
			}
			tableInfo, err := TableToProto(sessionCxt, fmt.Sprintf("%s.%s", schema.dbInfo.Name.O, schema.tableInfo.Name.O), *schema.tableInfo)
			if err != nil {
				return errors.Trace(err)
			}
			tableInfoBytes, err := tableInfo.Marshal()
			if err != nil {
				return errors.Trace(err)
			}
			s := &backuppb.Schema{
				Db:         dbBytes,
				Table:      tableBytes,
				TableInfo:  tableInfoBytes,
				Crc64Xor:   schema.crc64xor,
				TotalKvs:   schema.totalKvs,
				TotalBytes: schema.totalBytes,
				Stats:      statsBytes,
			}

			if err := metaWriter.Send(s, op); err != nil {
				return errors.Trace(err)
			}
			updateCh.Inc()
			return nil
		})
	}
	if err := errg.Wait(); err != nil {
		return errors.Trace(err)
	}
	log.Info("backup checksum", zap.Duration("take", time.Since(startAll)))
	summary.CollectDuration("backup checksum", time.Since(startAll))
	return metaWriter.FinishWriteMetas(ctx, op)
}

// Len returns the number of schemas.
func (ss *Schemas) Len() int {
	return len(ss.schemas)
}

func calculateChecksum(
	ctx context.Context,
	table *model.TableInfo,
	client kv.Client,
	backupTS uint64,
	concurrency uint,
) (*tipb.ChecksumResponse, error) {
	exe, err := checksum.NewExecutorBuilder(table, backupTS).
		SetConcurrency(concurrency).
		Build()
	if err != nil {
		return nil, errors.Trace(err)
	}
	checksumResp, err := exe.Execute(ctx, client, func() {
		// TODO: update progress here.
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	return checksumResp, nil
}
