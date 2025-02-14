package iavl

import (
	"context"
	dsql "database/sql"
	"fmt"
	"time"

	"github.com/dustin/go-humanize"
)

type sqliteBatch struct {
	tree   *Tree
	sql    *SqliteDb
	size   int64
	logger Logger

	treeCount int64
	treeSince time.Time
	leafCount int64
	leafSince time.Time

	treeTx       *dsql.Tx
	leafTx       *dsql.Tx
	leafInsert   *dsql.Stmt
	deleteInsert *dsql.Stmt
	latestInsert *dsql.Stmt
	latestDelete *dsql.Stmt
	treeInsert   *dsql.Stmt
	leafOrphan   *dsql.Stmt
	treeOrphan   *dsql.Stmt
}

func (b *sqliteBatch) newChangeLogBatch() (err error) {
	b.leafTx, err = b.sql.leafWrite.BeginTx(context.Background(), &dsql.TxOptions{})
	if err != nil {
		return err
	}
	b.leafInsert, err = b.leafTx.Prepare("INSERT OR REPLACE INTO leaf (version, sequence, bytes) VALUES (?, ?, ?)")
	if err != nil {
		return err
	}
	b.deleteInsert, err = b.leafTx.Prepare("INSERT OR REPLACE INTO leaf_delete (version, sequence, key) VALUES (?, ?, ?)")
	if err != nil {
		return err
	}
	b.latestInsert, err = b.leafTx.Prepare("INSERT OR REPLACE INTO latest (key, value) VALUES (?, ?)")
	if err != nil {
		return err
	}
	b.latestDelete, err = b.leafTx.Prepare("DELETE FROM latest WHERE key = ?")
	if err != nil {
		return err
	}
	b.leafOrphan, err = b.leafTx.Prepare("INSERT INTO leaf_orphan (version, sequence, at) VALUES (?, ?, ?)")
	if err != nil {
		return err
	}
	b.leafSince = time.Now()
	return nil
}

func (b *sqliteBatch) changelogMaybeCommit() (err error) {
	if b.leafCount%b.size == 0 {
		if err = b.changelogBatchCommit(); err != nil {
			return err
		}
		if err = b.newChangeLogBatch(); err != nil {
			return err
		}
	}
	return nil
}

func (b *sqliteBatch) changelogBatchCommit() error {
	if err := b.leafTx.Commit(); err != nil {
		return err
	}
	if err := b.leafInsert.Close(); err != nil {
		return err
	}
	if err := b.deleteInsert.Close(); err != nil {
		return err
	}
	if err := b.latestInsert.Close(); err != nil {
		return err
	}
	if err := b.latestDelete.Close(); err != nil {
		return err
	}
	if err := b.leafOrphan.Close(); err != nil {
		return err
	}

	return nil
}

func (b *sqliteBatch) execBranchOrphan(nodeKey NodeKey) error {
	_, err := b.treeOrphan.Exec(nodeKey.Version(), int(nodeKey.Sequence()), b.tree.version)
	return err
}

func (b *sqliteBatch) newTreeBatch(shardID int64) (err error) {
	b.treeTx, err = b.sql.treeWrite.BeginTx(context.Background(), &dsql.TxOptions{})
	if err != nil {
		return err
	}
	b.treeInsert, err = b.treeTx.Prepare(fmt.Sprintf(
		"INSERT INTO tree_%d (version, sequence, bytes) VALUES (?, ?, ?)", shardID))
	if err != nil {
		return err
	}
	b.treeOrphan, err = b.treeTx.Prepare("INSERT INTO orphan (version, sequence, at) VALUES (?, ?, ?)")
	b.treeSince = time.Now()
	return err
}

func (b *sqliteBatch) treeBatchCommit() error {
	if err := b.treeTx.Commit(); err != nil {
		return err
	}
	if err := b.treeInsert.Close(); err != nil {
		return err
	}
	if err := b.treeOrphan.Close(); err != nil {
		return err
	}

	if b.treeCount >= b.size {
		batchSize := b.treeCount % b.size
		if batchSize == 0 {
			batchSize = b.size
		}
		b.logger.Debug(fmt.Sprintf("db=tree count=%s dur=%s batch=%d rate=%s",
			humanize.Comma(b.treeCount),
			time.Since(b.treeSince).Round(time.Millisecond),
			batchSize,
			humanize.Comma(int64(float64(batchSize)/time.Since(b.treeSince).Seconds()))))
	}
	return nil
}

func (b *sqliteBatch) treeMaybeCommit(shardID int64) (err error) {
	if b.treeCount%b.size == 0 {
		if err = b.treeBatchCommit(); err != nil {
			return err
		}
		if err = b.newTreeBatch(shardID); err != nil {
			return err
		}
	}
	return nil
}

func (b *sqliteBatch) saveLeaves() (int64, error) {
	var byteCount int64

	err := b.newChangeLogBatch()
	if err != nil {
		return 0, err
	}

	var (
		bz   []byte
		val  []byte
		tree = b.tree
	)
	for i, leaf := range tree.leaves {
		b.leafCount++
		if tree.storeLatestLeaves {
			val = leaf.value
			leaf.value = nil
		}
		bz, err = leaf.Bytes()
		if err != nil {
			return 0, err
		}
		byteCount += int64(len(bz))
		if _, err = b.leafInsert.Exec(leaf.nodeKey.Version(), int(leaf.nodeKey.Sequence()), bz); err != nil {
			return 0, err
		}
		if tree.storeLatestLeaves {
			if _, err = b.latestInsert.Exec(leaf.key, val); err != nil {
				return 0, err
			}
		}
		if err = b.changelogMaybeCommit(); err != nil {
			return 0, err
		}
		if tree.heightFilter > 0 {
			if i != 0 {
				// evict leaf
				tree.returnNode(leaf)
			} else if leaf.nodeKey != tree.root.nodeKey {
				// never evict the root if it's a leaf
				tree.returnNode(leaf)
			}
		}
	}

	for _, leafDelete := range tree.deletes {
		b.leafCount++
		_, err = b.deleteInsert.Exec(leafDelete.deleteKey.Version(), int(leafDelete.deleteKey.Sequence()), leafDelete.leafKey)
		if err != nil {
			return 0, err
		}
		if tree.storeLatestLeaves {
			if _, err = b.latestDelete.Exec(leafDelete.leafKey); err != nil {
				return 0, err
			}
		}
		if err = b.changelogMaybeCommit(); err != nil {
			return 0, err
		}
	}

	for _, orphan := range tree.leafOrphans {
		b.leafCount++
		_, err = b.leafOrphan.Exec(orphan.Version(), int(orphan.Sequence()), b.tree.version)
		if err != nil {
			return 0, err
		}
		if err = b.changelogMaybeCommit(); err != nil {
			return 0, err
		}
	}

	if err = b.changelogBatchCommit(); err != nil {
		return 0, err
	}

	_, err = tree.sql.leafWrite.ExecContext(context.Background(), "CREATE UNIQUE INDEX IF NOT EXISTS leaf_idx ON leaf (version, sequence)")
	if err != nil {
		return byteCount, err
	}

	return byteCount, nil
}

func (b *sqliteBatch) isCheckpoint() bool {
	return len(b.tree.branches) > 0
}

func (b *sqliteBatch) saveBranches() (n int64, err error) {
	if b.isCheckpoint() {
		tree := b.tree
		b.treeCount = 0

		shardID, err := tree.sql.nextShard(tree.version)
		if err != nil {
			return 0, err
		}
		b.logger.Debug(fmt.Sprintf("checkpoint db=tree version=%d shard=%d orphans=%s",
			tree.version, shardID, humanize.Comma(int64(len(tree.branchOrphans)))))

		if err = b.newTreeBatch(shardID); err != nil {
			return 0, err
		}

		for _, node := range tree.branches {
			b.treeCount++
			bz, err := node.Bytes()
			if err != nil {
				return 0, err
			}
			if _, err = b.treeInsert.Exec(node.nodeKey.Version(), int(node.nodeKey.Sequence()), bz); err != nil {
				return 0, err
			}
			if err = b.treeMaybeCommit(shardID); err != nil {
				return 0, err
			}
			if node.evict {
				tree.returnNode(node)
			}
		}

		for _, orphan := range tree.branchOrphans {
			b.treeCount++
			err = b.execBranchOrphan(orphan)
			if err != nil {
				return 0, err
			}
			if err = b.treeMaybeCommit(shardID); err != nil {
				return 0, err
			}
		}

		if err = b.treeBatchCommit(); err != nil {
			return 0, err
		}
		_, err = b.sql.treeWrite.ExecContext(context.Background(), fmt.Sprintf(
			"CREATE INDEX IF NOT EXISTS tree_idx_%d ON tree_%d (version, sequence);", shardID, shardID))
		if err != nil {
			return 0, err
		}
	}

	return b.treeCount, nil
}
