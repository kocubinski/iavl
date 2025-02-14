package iavl

import (
	"bytes"
	"context"
	dsql "database/sql"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/cosmos/iavl/v2/metrics"
	"github.com/dustin/go-humanize"
	api "github.com/kocubinski/costor-api"

	// sqlite driver
	_ "github.com/mattn/go-sqlite3"
	// _ "modernc.org/sqlite"
)

const (
	defaultSQLitePath = "/tmp/iavl-v2"
	driverName        = "sqlite3"
)

type SqliteDbOptions struct {
	Path       string
	Mode       int
	MmapSize   uint64
	WalSize    int
	CacheSize  int
	ConnArgs   string
	ShardTrees bool

	Logger  Logger
	Metrics metrics.Proxy

	walPages int
}

type SqliteDb struct {
	opts SqliteDbOptions

	pool *NodePool

	// 2 separate databases and 2 separate connections.  the underlying databases have different WAL policies
	// therefore separation is required.
	leafWrite *dsql.Conn
	treeWrite *dsql.Conn

	// for latest table queries
	itrIdx      int
	iterators   map[int]*dsql.Rows
	queryLatest *dsql.Stmt

	readConn  *dsql.Conn
	queryLeaf *dsql.Stmt

	shards       *VersionRange
	shardQueries map[int64]*dsql.Stmt

	metrics metrics.Proxy
	logger  Logger
}

func defaultSqliteDbOptions(opts SqliteDbOptions) SqliteDbOptions {
	if opts.Path == "" {
		opts.Path = defaultSQLitePath
	}
	if opts.MmapSize == 0 {
		opts.MmapSize = 8 * 1024 * 1024 * 1024
	}
	if opts.WalSize == 0 {
		opts.WalSize = 1024 * 1024 * 100
	}
	if opts.Metrics == nil {
		opts.Metrics = metrics.NilMetrics{}
	}
	opts.walPages = opts.WalSize / os.Getpagesize()

	if opts.Logger == nil {
		opts.Logger = NewNopLogger()
	}

	return opts
}

func (opts SqliteDbOptions) connArgs() string {
	if opts.ConnArgs == "" {
		return ""
	}
	return fmt.Sprintf("?%s", opts.ConnArgs)
}

func (opts SqliteDbOptions) leafConnectionString() string {
	return fmt.Sprintf("file:%s/changelog.sqlite%s", opts.Path, opts.connArgs())
}

func (opts SqliteDbOptions) treeConnectionString() string {
	return fmt.Sprintf("file:%s/tree.sqlite%s", opts.Path, opts.connArgs())
}

func (opts SqliteDbOptions) EstimateMmapSize() (uint64, error) {
	opts.Logger.Info("calculate mmap size")
	opts.Logger.Info(fmt.Sprintf("leaf connection string: %s", opts.leafConnectionString()))
	db, err := dsql.Open(driverName, opts.leafConnectionString())
	if err != nil {
		return 0, err
	}
	var leafSize int64
	row := db.QueryRow("SELECT SUM(pgsize) FROM dbstat WHERE name = 'leaf'")
	err = row.Scan(&leafSize)
	if errors.Is(err, dsql.ErrNoRows) {
		return 0, err
	} else if err != nil {
		return 0, err
	}

	mmapSize := uint64(float64(leafSize) * 1.3)
	opts.Logger.Info(fmt.Sprintf("leaf mmap size: %s", humanize.Bytes(mmapSize)))

	return mmapSize, nil
}

func NewInMemorySqliteDb(pool *NodePool) (*SqliteDb, error) {
	opts := defaultSqliteDbOptions(SqliteDbOptions{ConnArgs: "mode=memory&cache=shared"})
	return NewSqliteDb(pool, opts)
}

func NewSqliteDb(pool *NodePool, opts SqliteDbOptions) (*SqliteDb, error) {
	opts = defaultSqliteDbOptions(opts)
	sql := &SqliteDb{
		shards:       &VersionRange{},
		shardQueries: make(map[int64]*dsql.Stmt),
		iterators:    make(map[int]*dsql.Rows),
		opts:         opts,
		pool:         pool,
		metrics:      opts.Metrics,
		logger:       opts.Logger,
	}

	if !api.IsFileExistent(opts.Path) {
		err := os.MkdirAll(opts.Path, 0755)
		if err != nil {
			return nil, err
		}
	}

	if err := sql.resetWriteConn(); err != nil {
		return nil, err
	}

	if err := sql.init(); err != nil {
		return nil, err
	}

	return sql, nil
}

func (sql *SqliteDb) init() error {
	ctx := context.Background()
	row := sql.treeWrite.QueryRowContext(ctx, "SELECT 1 from sqlite_master WHERE type='table' AND name='root'")
	var i int
	err := row.Scan(&i)
	noRow := errors.Is(err, dsql.ErrNoRows)
	if err != nil && !noRow {
		return err
	}
	if noRow {
		_, err = sql.treeWrite.ExecContext(ctx, `
CREATE TABLE orphan (version int, sequence int, at int);
CREATE INDEX orphan_idx ON orphan (at);
CREATE TABLE root (
	version int, 
	node_version int, 
	node_sequence int, 
	bytes blob, 
	checkpoint bool, 
	PRIMARY KEY (version))`)
		if err != nil {
			return err
		}

		pageSize := os.Getpagesize()
		sql.logger.Info(fmt.Sprintf("setting page size to %s", humanize.Bytes(uint64(pageSize))))
		_, err = sql.treeWrite.ExecContext(ctx, fmt.Sprintf("PRAGMA page_size=%d; VACUUM;", pageSize))
		if err != nil {
			return err
		}
		_, err = sql.treeWrite.ExecContext(ctx, "PRAGMA journal_mode=WAL;")
		if err != nil {
			return err
		}
	}

	row = sql.leafWrite.QueryRowContext(ctx, "SELECT 1 from sqlite_master WHERE type='table' AND name='leaf'")
	err = row.Scan(&i)
	noRow = errors.Is(err, dsql.ErrNoRows)
	if err != nil && !noRow {
		return err
	}
	if noRow {
		_, err = sql.leafWrite.ExecContext(ctx, `
CREATE TABLE latest (key blob, value blob, PRIMARY KEY (key));
CREATE TABLE leaf (version int, sequence int, bytes blob, orphaned bool);
CREATE TABLE leaf_delete (version int, sequence int, key blob, PRIMARY KEY (version, sequence));
CREATE TABLE leaf_orphan (version int, sequence int, at int);
CREATE INDEX leaf_orphan_idx ON leaf_orphan (at);`)
		if err != nil {
			return err
		}

		pageSize := os.Getpagesize()
		sql.logger.Info(fmt.Sprintf("setting page size to %s", humanize.Bytes(uint64(pageSize))))
		_, err = sql.leafWrite.ExecContext(ctx, fmt.Sprintf("PRAGMA page_size=%d; VACUUM;", pageSize))
		if err != nil {
			return err
		}
		_, err = sql.leafWrite.ExecContext(ctx, "PRAGMA journal_mode=WAL;")
		if err != nil {
			return err
		}
	}

	return nil
}

func (sql *SqliteDb) resetWriteConn() (err error) {
	if sql.treeWrite != nil {
		err = sql.treeWrite.Close()
		if err != nil {
			return err
		}
	}
	db, err := dsql.Open(driverName, sql.opts.treeConnectionString())
	if err != nil {
		return err
	}
	ctx := context.Background()
	sql.treeWrite, err = db.Conn(ctx)
	if err != nil {
		return err
	}

	_, err = sql.treeWrite.ExecContext(ctx, "PRAGMA synchronous=OFF;")
	if err != nil {
		return err
	}

	if _, err = sql.treeWrite.ExecContext(ctx, fmt.Sprintf("PRAGMA wal_autocheckpoint=%d", sql.opts.walPages)); err != nil {
		return err
	}

	db, err = dsql.Open(driverName, sql.opts.leafConnectionString())
	if err != nil {
		return err
	}
	sql.leafWrite, err = db.Conn(ctx)
	if err != nil {
		return err
	}

	_, err = sql.leafWrite.ExecContext(ctx, "PRAGMA synchronous=OFF;")
	if err != nil {
		return err
	}

	if _, err = sql.leafWrite.ExecContext(ctx, fmt.Sprintf("PRAGMA wal_autocheckpoint=%d", sql.opts.walPages)); err != nil {
		return err
	}

	return err
}

func (sql *SqliteDb) newReadConn() (*dsql.Conn, error) {
	db, err := dsql.Open(driverName, sql.opts.treeConnectionString())
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	_, err = conn.ExecContext(ctx, fmt.Sprintf("ATTACH DATABASE '%s' AS changelog;", sql.opts.leafConnectionString()))
	if err != nil {
		return nil, err
	}
	_, err = conn.ExecContext(ctx, fmt.Sprintf("PRAGMA mmap_size=%d;", sql.opts.MmapSize))
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (sql *SqliteDb) resetReadConn() (err error) {
	if sql.readConn != nil {
		err = sql.readConn.Close()
		if err != nil {
			return err
		}
	}
	sql.readConn, err = sql.newReadConn()
	return err
}

func (sql *SqliteDb) getReadConn() (*dsql.Conn, error) {
	var err error
	if sql.readConn == nil {
		sql.readConn, err = sql.newReadConn()
	}
	return sql.readConn, err
}

func (sql *SqliteDb) getLeaf(nodeKey NodeKey) (*Node, error) {
	start := time.Now()
	defer func() {
		sql.metrics.MeasureSince(start, metricsNamespace, "db_get")
		sql.metrics.IncrCounter(1, metricsNamespace, "db_get_leaf")
	}()
	var (
		err    error
		nodeBz []byte
	)
	if sql.queryLeaf == nil {
		sql.queryLeaf, err = sql.readConn.PrepareContext(context.Background(), "SELECT bytes FROM changelog.leaf WHERE version = ? AND sequence = ?")
		if err != nil {
			return nil, err
		}
	}
	row := sql.queryLeaf.QueryRow(nodeKey.Version(), int(nodeKey.Sequence()))
	err = row.Scan(&nodeBz)
	if errors.Is(err, dsql.ErrNoRows) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return MakeNode(sql.pool, nodeKey, nodeBz)
}

func (sql *SqliteDb) getNode(nodeKey NodeKey) (*Node, error) {
	start := time.Now()
	q, err := sql.getShardQuery(nodeKey.Version())
	if err != nil {
		return nil, err
	}
	defer func() {
		sql.metrics.MeasureSince(start, metricsNamespace, "db_get")
		sql.metrics.IncrCounter(1, metricsNamespace, "db_get_branch")
	}()
	var nodeBz []byte
	row := q.QueryRow(nodeKey.Version(), int(nodeKey.Sequence()))
	err = row.Scan(&nodeBz)
	if errors.Is(err, dsql.ErrNoRows) {
		return nil, fmt.Errorf("node not found: %v; shard=%d; path=%s", nodeKey, sql.shards.Find(nodeKey.Version()), sql.opts.Path)
	} else if err != nil {
		return nil, err
	}
	return MakeNode(sql.pool, nodeKey, nodeBz)
}

func (sql *SqliteDb) Close() error {
	for _, q := range sql.shardQueries {
		err := q.Close()
		if err != nil {
			return err
		}
	}
	if sql.readConn != nil {
		if sql.queryLeaf != nil {
			if err := sql.queryLeaf.Close(); err != nil {
				return err
			}
		}
		if err := sql.readConn.Close(); err != nil {
			return err
		}
	}
	if err := sql.leafWrite.Close(); err != nil {
		return err
	}

	if err := sql.treeWrite.Close(); err != nil {
		return err
	}
	return nil
}

func (sql *SqliteDb) nextShard(version int64) (int64, error) {
	if !sql.opts.ShardTrees {
		switch sql.shards.Len() {
		case 0:
			break
		case 1:
			return sql.shards.Last(), nil
		default:
			return -1, fmt.Errorf("sharding is disabled but found shards; shards=%v", sql.shards.versions)
		}
	}

	sql.logger.Info(fmt.Sprintf("creating shard %d", version))
	ctx := context.Background()
	_, err := sql.treeWrite.ExecContext(ctx, fmt.Sprintf("CREATE TABLE tree_%d (version int, sequence int, bytes blob, orphaned bool);", version))
	if err != nil {
		return version, err
	}
	return version, sql.shards.Add(version)
}

func (sql *SqliteDb) SaveRoot(version int64, node *Node, isCheckpoint bool) (topErr error) {
	ctx := context.Background()
	if node != nil {
		bz, err := node.Bytes()
		if err != nil {
			return err
		}
		_, topErr = sql.treeWrite.ExecContext(ctx, "INSERT OR REPLACE INTO root(version, node_version, node_sequence, bytes, checkpoint) VALUES (?, ?, ?, ?, ?)",
			version, node.nodeKey.Version(), int(node.nodeKey.Sequence()), bz, isCheckpoint)
	} else {
		// for an empty root a sentinel is saved
		_, topErr = sql.treeWrite.ExecContext(ctx, "INSERT OR REPLACE INTO root(version, checkpoint) VALUES (?, ?)", version, isCheckpoint)
	}
	return topErr
}

func (sql *SqliteDb) LoadRoot(version int64) (*Node, error) {
	db, err := dsql.Open(driverName, sql.opts.treeConnectionString())
	if err != nil {
		return nil, err
	}
	var (
		nodeSeq     int
		nodeVersion int64
		nodeBz      []byte
	)
	row := db.QueryRow("SELECT node_version, node_sequence, bytes FROM root WHERE version = ?", version)
	err = row.Scan(&nodeVersion, &nodeSeq, &nodeBz)
	if errors.Is(err, dsql.ErrNoRows) {
		return nil, fmt.Errorf("root not found for version %d", version)
	} else if err != nil {
		return nil, err
	}

	// if nodeBz is nil then a (valid) empty tree was saved, which a nil root represents
	var root *Node
	if nodeBz != nil {
		rootKey := NewNodeKey(nodeVersion, uint32(nodeSeq))
		root, err = MakeNode(sql.pool, rootKey, nodeBz)
		if err != nil {
			return nil, err
		}
	}

	if err := sql.ResetShardQueries(); err != nil {
		return nil, err
	}
	if err := db.Close(); err != nil {
		return nil, err
	}
	return root, nil
}

// lastCheckpoint fetches the last checkpoint version from the shard table previous to the loaded root's version.
// a return value of zero and nil error indicates no checkpoint was found.
func (sql *SqliteDb) lastCheckpoint(treeVersion int64) (checkpointVersion int64, err error) {
	db, err := dsql.Open(driverName, sql.opts.treeConnectionString())
	if err != nil {
		return 0, err
	}
	row := db.QueryRow("SELECT MAX(version) FROM root WHERE checkpoint = true AND version <= ?", treeVersion)
	err = row.Scan(&checkpointVersion)
	if errors.Is(err, dsql.ErrNoRows) {
		return 0, nil
	} else if err != nil {
		return 0, err
	}
	if err = db.Close(); err != nil {
		return 0, err
	}
	return checkpointVersion, nil
}

func (sql *SqliteDb) loadCheckpointRange() (versionRange *VersionRange, topErr error) {
	db, err := dsql.Open(driverName, sql.opts.treeConnectionString())
	if err != nil {
		return nil, err
	}
	rows, err := db.Query("SELECT version FROM root WHERE checkpoint = true ORDER BY version")
	defer func() {
		topErr = errors.Join(topErr, rows.Close(), db.Close())
	}()
	if err != nil {
		return nil, err
	}
	var version int64
	versionRange = &VersionRange{}
	for rows.Next() {
		err = rows.Scan(&version)
		if err != nil {
			return nil, err
		}
		if err = versionRange.Add(version); err != nil {
			return nil, err

		}
	}
	if rows.Err() != nil {
		return nil, err
	}
	return versionRange, nil
}

func (sql *SqliteDb) getShard(version int64) (int64, error) {
	if !sql.opts.ShardTrees {
		if sql.shards.Len() != 1 {
			return -1, fmt.Errorf("expected a single shard; path=%s", sql.opts.Path)
		}
		return sql.shards.Last(), nil
	}
	v := sql.shards.FindMemoized(version)
	if v == -1 {
		return -1, fmt.Errorf("version %d is after the first shard; shards=%v", version, sql.shards.versions)
	}
	return v, nil
}

func (sql *SqliteDb) getShardQuery(version int64) (*dsql.Stmt, error) {
	v, err := sql.getShard(version)
	if err != nil {
		return nil, err
	}

	if q, ok := sql.shardQueries[v]; ok {
		return q, nil
	}
	sqlQuery := fmt.Sprintf("SELECT bytes FROM tree_%d WHERE version = ? AND sequence = ?", v)
	ctx := context.Background()
	q, err := sql.readConn.PrepareContext(ctx, sqlQuery)
	if err != nil {
		return nil, err
	}
	sql.shardQueries[v] = q
	sql.logger.Debug(fmt.Sprintf("added shard query: %s", sqlQuery))
	return q, nil
}

func (sql *SqliteDb) ResetShardQueries() (topErr error) {
	for k, q := range sql.shardQueries {
		err := q.Close()
		if err != nil {
			return err
		}
		delete(sql.shardQueries, k)
	}

	sql.shards = &VersionRange{}

	if sql.readConn == nil {
		if err := sql.resetReadConn(); err != nil {
			return err
		}
	}

	ctx := context.Background()
	rows, err := sql.treeWrite.QueryContext(ctx, "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'tree_%'")
	defer func() {
		topErr = errors.Join(topErr, rows.Close())
	}()
	if err != nil {
		return err
	}
	for rows.Next() {
		var shard string
		err = rows.Scan(&shard)
		if err != nil {
			return err
		}
		shardVersion, err := strconv.Atoi(shard[5:])
		if err != nil {
			return err
		}
		if err = sql.shards.Add(int64(shardVersion)); err != nil {
			return fmt.Errorf("failed to add shard path=%s: %w", sql.opts.Path, err)
		}
	}
	return rows.Err()
}

func (sql *SqliteDb) WarmLeaves() (topErr error) {
	start := time.Now()
	read, err := sql.getReadConn()
	if err != nil {
		return err
	}
	ctx := context.Background()
	rows, err := read.QueryContext(ctx, "SELECT version, sequence, bytes FROM leaf")
	defer func() {
		topErr = errors.Join(topErr, rows.Close())
	}()
	if err != nil {
		return err
	}
	var (
		cnt, version, seq int64
		kz, vz            []byte
	)
	for rows.Next() {
		cnt++
		err = rows.Scan(&version, &seq, &vz)
		if err != nil {
			return err
		}
		if cnt%5_000_000 == 0 {
			sql.logger.Info(fmt.Sprintf("warmed %s leaves", humanize.Comma(cnt)))
		}
	}
	if rows.Err() != nil {
		return rows.Err()
	}
	rows, err = read.QueryContext(ctx, "SELECT key, value FROM latest")
	defer func() {
		topErr = errors.Join(topErr, rows.Close())
	}()
	if err != nil {
		return err
	}
	for rows.Next() {
		cnt++
		err = rows.Scan(&kz, &vz)
		if err != nil {
			return err
		}
		if cnt%5_000_000 == 0 {
			sql.logger.Info(fmt.Sprintf("warmed %s leaves", humanize.Comma(cnt)))
		}
	}

	sql.logger.Info(fmt.Sprintf("warmed %s leaves in %s", humanize.Comma(cnt), time.Since(start)))
	return rows.Err()
}

func isLeafSeq(seq uint32) bool {
	return seq&(1<<31) != 0
}

func (sql *SqliteDb) getRightNode(node *Node) (*Node, error) {
	if node.isLeaf() {
		return nil, errors.New("leaf node has no children")
	}
	var err error
	if isLeafSeq(node.rightNodeKey.Sequence()) {
		node.rightNode, err = sql.getLeaf(node.rightNodeKey)
	} else {
		node.rightNode, err = sql.getNode(node.rightNodeKey)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get right node node_key=%s height=%d path=%s: %w",
			node.rightNodeKey, node.subtreeHeight, sql.opts.Path, err)
	}
	if node.rightNode == nil {
		err = errors.New("not found")
	}
	return node.rightNode, nil
}

func (sql *SqliteDb) getLeftNode(node *Node) (*Node, error) {
	if node.isLeaf() {
		return nil, errors.New("leaf node has no children")
	}
	var err error
	if isLeafSeq(node.leftNodeKey.Sequence()) {
		node.leftNode, err = sql.getLeaf(node.leftNodeKey)
	} else {
		node.leftNode, err = sql.getNode(node.leftNodeKey)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get left node node_key=%s height=%d path=%s: %w",
			node.leftNodeKey, node.subtreeHeight, sql.opts.Path, err)
	}
	if node.leftNode == nil {
		err = errors.New("not found")
	}
	return node.leftNode, nil
}

func (sql *SqliteDb) isSharded() (bool, error) {
	ctx := context.Background()
	row := sql.treeWrite.QueryRowContext(ctx, "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'tree_%'")
	err := row.Scan()
	if errors.Is(err, dsql.ErrNoRows) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, nil
}

func (sql *SqliteDb) Revert(version int) (topErr error) {
	ctx := context.Background()
	if _, err := sql.leafWrite.ExecContext(ctx, "DELETE FROM leaf WHERE version > ?", version); err != nil {
		return err
	}
	if _, err := sql.leafWrite.ExecContext(ctx, "DELETE FROM leaf_delete WHERE version > ?", version); err != nil {
		return err
	}
	if _, err := sql.leafWrite.ExecContext(ctx, "DELETE FROM leaf_orphan WHERE at > ?", version); err != nil {
		return err
	}
	if _, err := sql.treeWrite.ExecContext(ctx, "DELETE FROM root WHERE version > ?", version); err != nil {
		return err
	}
	if _, err := sql.treeWrite.ExecContext(ctx, "DELETE FROM orphan WHERE at > ?", version); err != nil {
		return err
	}

	hasShards, err := sql.isSharded()
	if err != nil {
		return err
	}
	if hasShards {
		rows, err := sql.treeWrite.QueryContext(ctx, "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'tree_%'")
		defer func() {
			topErr = errors.Join(topErr, rows.Err())
		}()
		if err != nil {
			return err
		}
		var shards []string
		for rows.Next() {
			var shard string
			err = rows.Scan(&shard)
			if err != nil {
				return err
			}
			shardVersion, err := strconv.Atoi(shard[5:])
			if err != nil {
				return err
			}
			if shardVersion > version {
				shards = append(shards, shard)
			}
		}
		if rows.Err() != nil {
			return rows.Err()
		}
		for _, shard := range shards {
			if _, err = sql.treeWrite.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", shard)); err != nil {
				return err
			}
		}
	} else {

	}
	return nil
}

func (sql *SqliteDb) GetLatestLeaf(key []byte) ([]byte, error) {
	ctx := context.Background()
	if sql.queryLatest == nil {
		var err error
		sql.queryLatest, err = sql.readConn.PrepareContext(ctx, "SELECT value FROM changelog.latest WHERE key = ?")
		if err != nil {
			return nil, err
		}
	}

	row := sql.queryLatest.QueryRowContext(ctx, key)
	var val []byte
	err := row.Scan(&val)
	if errors.Is(err, dsql.ErrNoRows) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return val, nil
}

func (sql *SqliteDb) closeHangingIterators() error {
	for idx, stmt := range sql.iterators {
		sql.logger.Warn(fmt.Sprintf("closing hanging iterator idx=%d", idx))
		if err := stmt.Close(); err != nil {
			return err
		}
		delete(sql.iterators, idx)
	}
	sql.itrIdx = 0
	return nil
}

func (sql *SqliteDb) getLeafIteratorQuery(start, end []byte, ascending, _ bool) (rows *dsql.Rows, idx int, err error) {
	var suffix string
	if ascending {
		suffix = "ASC"
	} else {
		suffix = "DESC"
	}

	conn, err := sql.getReadConn()
	if err != nil {
		return nil, idx, err
	}

	sql.itrIdx++
	idx = sql.itrIdx
	ctx := context.Background()

	switch {
	case start == nil && end == nil:
		rows, err = conn.QueryContext(
			ctx, fmt.Sprintf("SELECT key, value FROM changelog.latest ORDER BY key %s", suffix))
		if err != nil {
			return nil, idx, err
		}
	case start == nil:
		rows, err = conn.QueryContext(
			ctx, fmt.Sprintf("SELECT key, value FROM changelog.latest WHERE key < ? ORDER BY key %s", suffix), end)
		if err != nil {
			return nil, idx, err
		}
	case end == nil:
		rows, err = conn.QueryContext(
			ctx, fmt.Sprintf("SELECT key, value FROM changelog.latest WHERE key >= ? ORDER BY key %s", suffix), start)
		if err != nil {
			return nil, idx, err
		}
	default:
		rows, err = conn.QueryContext(
			ctx, fmt.Sprintf("SELECT key, value FROM changelog.latest WHERE key >= ? AND key < ? ORDER BY key %s", suffix), start, end)
		if err != nil {
			return nil, idx, err
		}
	}

	sql.iterators[idx] = rows
	return rows, idx, err
}

func (sql *SqliteDb) replayChangelog(tree *Tree, toVersion int64, targetHash []byte) (topErr error) {
	var (
		version     int
		lastVersion int
		sequence    int
		bz          []byte
		key         []byte
		count       int64
		start       = time.Now()
		since       = time.Now()
		logPath     = []interface{}{"path", sql.opts.Path}
	)
	tree.isReplaying = true
	defer func() {
		tree.isReplaying = false
	}()

	ctx := context.Background()
	sql.opts.Logger.Info("ensure leaf_delete_index exists...", logPath...)
	if _, err := sql.leafWrite.ExecContext(ctx, "CREATE UNIQUE INDEX IF NOT EXISTS leaf_delete_idx ON leaf_delete (version, sequence)"); err != nil {
		return err
	}
	sql.opts.Logger.Info("...done", logPath...)
	sql.opts.Logger.Info(fmt.Sprintf("replaying changelog from=%d to=%d", tree.version, toVersion), logPath...)
	conn, err := sql.getReadConn()
	if err != nil {
		return err
	}
	rows, err := conn.QueryContext(ctx, `SELECT * FROM (
		SELECT version, sequence, bytes, null AS key
	FROM leaf WHERE version > ? AND version <= ?
	UNION
	SELECT version, sequence, null as bytes, key
	FROM leaf_delete WHERE version > ? AND version <= ?
	) as ops
	ORDER BY version, sequence`,
		tree.version, toVersion, tree.version, toVersion)
	defer func() {
		topErr = errors.Join(topErr, rows.Close())
	}()
	if err != nil {
		return err
	}
	for rows.Next() {
		count++
		if err = rows.Scan(&version, &sequence, &bz, &key); err != nil {
			return err
		}
		if version-1 != lastVersion {
			tree.leaves, tree.branches, tree.leafOrphans, tree.deletes = nil, nil, nil, nil
			tree.version = int64(version - 1)
			tree.resetSequences()
			lastVersion = version - 1
		}
		if bz != nil {
			nk := NewNodeKey(0, 0)
			node, err := MakeNode(tree.pool, nk, bz)
			if err != nil {
				return err
			}
			if _, err = tree.Set(node.key, node.hash); err != nil {
				return err
			}
			if sequence != int(tree.leafSequence) {
				return fmt.Errorf("sequence mismatch version=%d; expected %d got %d; path=%s",
					version, sequence, tree.leafSequence, sql.opts.Path)
			}
		} else {
			if _, _, err = tree.Remove(key); err != nil {
				return err
			}
			deleteSequence := tree.deletes[len(tree.deletes)-1].deleteKey.Sequence()
			if sequence != int(deleteSequence) {
				return fmt.Errorf("sequence delete mismatch; version=%d expected %d got %d; path=%s",
					version, sequence, tree.leafSequence, sql.opts.Path)
			}
		}
		if count%250_000 == 0 {
			sql.opts.Logger.Info(fmt.Sprintf("replayed changelog to version=%d count=%s node/s=%s",
				version, humanize.Comma(count), humanize.Comma(int64(250_000/time.Since(since).Seconds()))), logPath)
			since = time.Now()
		}
	}
	rootHash := tree.computeHash()
	if !bytes.Equal(targetHash, rootHash) {
		return fmt.Errorf("root hash mismatch; expected %x got %x", targetHash, rootHash)
	}
	tree.leaves, tree.branches, tree.leafOrphans, tree.deletes = nil, nil, nil, nil
	tree.resetSequences()
	tree.version = toVersion
	sql.opts.Logger.Info(fmt.Sprintf("replayed changelog to version=%d count=%s dur=%s root=%v",
		tree.version, humanize.Comma(count), time.Since(start).Round(time.Millisecond), tree.root), logPath)
	return rows.Close()
}

func (sql *SqliteDb) WriteLatestLeaves(tree *Tree) (err error) {
	var (
		since        = time.Now()
		batchSize    = 200_000
		count        = 0
		step         func(node *Node) error
		logPath      = []string{"path", sql.opts.Path}
		latestInsert *dsql.Stmt
		ctx          = context.Background()
		tx           *dsql.Tx
	)
	prepare := func() error {
		if tx, err = sql.leafWrite.BeginTx(ctx, &dsql.TxOptions{}); err != nil {
			return err
		}
		latestInsert, err = tx.Prepare("INSERT INTO latest (key, value) VALUES (?, ?)")
		if err != nil {
			return err
		}
		return nil
	}

	flush := func() error {
		if err = tx.Commit(); err != nil {
			return err
		}
		if err = latestInsert.Close(); err != nil {
			return err
		}
		var rate string
		if time.Since(since).Seconds() > 0 {
			rate = humanize.Comma(int64(float64(batchSize) / time.Since(since).Seconds()))
		} else {
			rate = "n/a"
		}
		sql.logger.Info(fmt.Sprintf("latest flush; count=%s dur=%s wr/s=%s",
			humanize.Comma(int64(count)),
			time.Since(since).Round(time.Millisecond),
			rate,
		), logPath)
		since = time.Now()
		return nil
	}

	maybeFlush := func() error {
		count++
		if count%batchSize == 0 {
			err = flush()
			if err != nil {
				return err
			}
			return prepare()
		}
		return nil
	}

	if err = prepare(); err != nil {
		return err
	}

	step = func(node *Node) error {
		if node.isLeaf() {
			_, err := latestInsert.Exec(node.key, node.value)
			if err != nil {
				return err
			}
			return maybeFlush()
		}
		if err = step(node.left(tree)); err != nil {
			return err
		}
		if err = step(node.right(tree)); err != nil {
			return err
		}
		return nil
	}

	err = step(tree.root)
	if err != nil {
		return err
	}
	err = flush()
	if err != nil {
		return err
	}

	return latestInsert.Close()
}

func (sql *SqliteDb) Logger() Logger {
	return sql.logger
}
