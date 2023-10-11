//go:build rocksdb
// +build rocksdb

package db

import (
	"fmt"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/linxGnu/grocksdb"
)

const BlockCacheSize = 1 << 30

func init() {
	dbCreator := func(name string, dir string) (DB, error) {
		return NewRocksDB(name, dir)
	}
	registerDBCreator(RocksDBBackend, dbCreator, false)
}

// RocksDB is a RocksDB backend.
type RocksDB struct {
	db     *grocksdb.DB
	ro     *grocksdb.ReadOptions
	wo     *grocksdb.WriteOptions
	woSync *grocksdb.WriteOptions
}

var _ DB = (*RocksDB)(nil)

func NewRocksDB(name string, dir string) (*RocksDB, error) {
	opts, err := loadLatestOptions(dir)
	if err != nil {
		return nil, err
	}
	// customize rocksdb options
	opts = NewRocksdbOptions(opts)
	return NewRocksDBWithOptions(name, dir, opts)
}

func NewRocksDBWithOptions(name string, dir string, opts *grocksdb.Options) (*RocksDB, error) {
	dbPath := filepath.Join(dir, name+".db")
	db, err := grocksdb.OpenDb(opts, dbPath)
	if err != nil {
		return nil, err
	}
	ro := grocksdb.NewDefaultReadOptions()
	wo := grocksdb.NewDefaultWriteOptions()
	woSync := grocksdb.NewDefaultWriteOptions()
	woSync.SetSync(true)
	return NewRocksDBWithRawDB(db, ro, wo, woSync), nil
}

func NewRocksDBWithRawDB(db *grocksdb.DB, ro *grocksdb.ReadOptions, wo *grocksdb.WriteOptions, woSync *grocksdb.WriteOptions) *RocksDB {
	return &RocksDB{
		db:     db,
		ro:     ro,
		wo:     wo,
		woSync: woSync,
	}
}

// Get implements DB.
func (db *RocksDB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, errKeyEmpty
	}
	res, err := db.db.Get(db.ro, key)
	if err != nil {
		return nil, err
	}
	return moveSliceToBytes(res), nil
}

// Has implements DB.
func (db *RocksDB) Has(key []byte) (bool, error) {
	bytes, err := db.Get(key)
	if err != nil {
		return false, err
	}
	return bytes != nil, nil
}

// Set implements DB.
func (db *RocksDB) Set(key []byte, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}
	err := db.db.Put(db.wo, key, value)
	if err != nil {
		return err
	}
	return nil
}

// SetSync implements DB.
func (db *RocksDB) SetSync(key []byte, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}
	err := db.db.Put(db.woSync, key, value)
	if err != nil {
		return err
	}
	return nil
}

// Delete implements DB.
func (db *RocksDB) Delete(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	err := db.db.Delete(db.wo, key)
	if err != nil {
		return err
	}
	return nil
}

// DeleteSync implements DB.
func (db *RocksDB) DeleteSync(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	err := db.db.Delete(db.woSync, key)
	if err != nil {
		return nil
	}
	return nil
}

func (db *RocksDB) DB() *grocksdb.DB {
	return db.db
}

// Close implements DB.
func (db *RocksDB) Close() error {
	db.ro.Destroy()
	db.wo.Destroy()
	db.woSync.Destroy()
	db.db.Close()
	return nil
}

// Print implements DB.
func (db *RocksDB) Print() error {
	itr, err := db.Iterator(nil, nil)
	if err != nil {
		return err
	}
	defer itr.Close()
	for ; itr.Valid(); itr.Next() {
		key := itr.Key()
		value := itr.Value()
		fmt.Printf("[%X]:\t[%X]\n", key, value)
	}
	return nil
}

// Stats implements DB.
func (db *RocksDB) Stats() map[string]string {
	keys := []string{"rocksdb.stats"}
	stats := make(map[string]string, len(keys))
	for _, key := range keys {
		stats[key] = db.db.GetProperty(key)
	}
	return stats
}

// NewBatch implements DB.
func (db *RocksDB) NewBatch() Batch {
	return newRocksDBBatch(db)
}

// Iterator implements DB.
func (db *RocksDB) Iterator(start, end []byte) (Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errKeyEmpty
	}
	itr := db.db.NewIterator(db.ro)
	return newRocksDBIterator(itr, start, end, false), nil
}

// ReverseIterator implements DB.
func (db *RocksDB) ReverseIterator(start, end []byte) (Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errKeyEmpty
	}
	itr := db.db.NewIterator(db.ro)
	return newRocksDBIterator(itr, start, end, true), nil
}

// loadLatestOptions try to load options from existing db, returns nil if not exists.
func loadLatestOptions(dir string) (*grocksdb.Options, error) {
	opts, err := grocksdb.LoadLatestOptions(dir, grocksdb.NewDefaultEnv(), true, grocksdb.NewLRUCache(BlockCacheSize))
	if err != nil {
		// not found is not an error
		if strings.HasPrefix(err.Error(), "NotFound: ") {
			return nil, nil
		}
		return nil, err
	}

	cfNames := opts.ColumnFamilyNames()
	cfOpts := opts.ColumnFamilyOpts()

	for i := 0; i < len(cfNames); i++ {
		if cfNames[i] == "default" {
			return &cfOpts[i], nil
		}
	}

	return opts.Options(), nil
}

// NewRocksdbOptions build options for `application.db`,
// it overrides existing options if provided, otherwise create new one assuming it's a new database.
func NewRocksdbOptions(opts *grocksdb.Options) *grocksdb.Options {
	if opts == nil {
		opts = grocksdb.NewDefaultOptions()
		// only enable dynamic-level-bytes on new db, don't override for existing db
		opts.SetLevelCompactionDynamicLevelBytes(true)
	}
	opts.SetCreateIfMissing(true)
	opts.IncreaseParallelism(runtime.NumCPU())
	opts.OptimizeLevelStyleCompaction(512 * 1024 * 1024)
	opts.SetTargetFileSizeMultiplier(2)

	// block based table options
	bbto := grocksdb.NewDefaultBlockBasedTableOptions()

	// 1G block cache
	bbto.SetBlockCache(grocksdb.NewLRUCache(BlockCacheSize))

	// http://rocksdb.org/blog/2021/12/29/ribbon-filter.html
	bbto.SetFilterPolicy(grocksdb.NewRibbonHybridFilterPolicy(9.9, 1))

	// partition index
	// http://rocksdb.org/blog/2017/05/12/partitioned-index-filter.html
	bbto.SetIndexType(grocksdb.KTwoLevelIndexSearchIndexType)
	bbto.SetPartitionFilters(true)
	bbto.SetOptimizeFiltersForMemory(true)
	// reduce memory usage
	bbto.SetCacheIndexAndFilterBlocks(true)
	bbto.SetPinTopLevelIndexAndFilter(true)

	opts.SetBlockBasedTableFactory(bbto)

	// heavier compression option at bottommost level,
	// 110k dict bytes is default in zstd library,
	// train bytes is recommended to be set at 100x dict bytes.
	opts.SetBottommostCompression(grocksdb.ZSTDCompression)
	compressOpts := grocksdb.NewDefaultCompressionOptions()
	compressOpts.Level = 12
	compressOpts.MaxDictBytes = 110 * 1024
	opts.SetBottommostCompressionOptionsZstdMaxTrainBytes(compressOpts.MaxDictBytes*100, true)
	opts.SetBottommostCompressionOptions(compressOpts, true)
	return opts
}
