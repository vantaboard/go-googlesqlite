package googlesqlengine

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log/slog"
	"sync"

	"google.golang.org/api/bigquery/v2"
	internal "github.com/vantaboard/go-googlesql-engine/internal"
	_ "modernc.org/sqlite"
)

var (
	_ driver.Driver = &GoogleSQLEngineDriver{}
	_ driver.Conn   = &GoogleSQLEngineConn{}
	_ driver.Tx     = &GoogleSQLEngineTx{}
)

var (
	nameToCatalogMap = map[string]*internal.Catalog{}
	nameToDBMap      = map[string]*sql.DB{}
	nameToValueMapMu sync.Mutex
)

func dbPoolKey(driverName, dsn string) string {
	return driverName + "\x00" + dsn
}

func init() {
	if err := internal.RegisterFunctions(); err != nil {
		slog.Error("googlesqlengine: failed to register functions", "err", err)
	}

	sql.Register("googlesqlengine", &GoogleSQLEngineDriver{})
}

func newDBAndCatalog(name string) (*sql.DB, *internal.Catalog, error) {
	return newDBAndCatalogWithBackend(name, internal.SQLiteBackend{})
}

func newDBAndCatalogWithBackend(dsn string, backend internal.SQLBackend) (*sql.DB, *internal.Catalog, error) {
	key := dbPoolKey(backend.DriverName(), dsn)
	nameToValueMapMu.Lock()
	defer nameToValueMapMu.Unlock()
	db, exists := nameToDBMap[key]
	if exists {
		return db, nameToCatalogMap[key], nil
	}
	db, err := internal.OpenSQLBackend(backend, dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open database %s (%s): %w", backend.DriverName(), dsn, err)
	}
	catalog, err := internal.NewCatalogWithRepository(db, backend.NewCatalogRepository())
	if err != nil {
		_ = db.Close()
		return nil, nil, fmt.Errorf("failed open database by %s: failed to initialize catalog: %w", dsn, err)
	}
	nameToDBMap[key] = db
	nameToCatalogMap[key] = catalog
	return db, catalog, nil
}

type GoogleSQLEngineDriver struct {
	ConnectHook func(*GoogleSQLEngineConn) error
}

func (d *GoogleSQLEngineDriver) Open(name string) (driver.Conn, error) {
	db, catalog, err := newDBAndCatalog(name)
	if err != nil {
		return nil, err
	}
	conn, err := newGoogleSQLEngineConn(db, catalog, internal.SQLiteDialect{})
	if err != nil {
		return nil, err
	}
	if d.ConnectHook != nil {
		if err := d.ConnectHook(conn); err != nil {
			return nil, err
		}
	}
	return conn, nil
}

type GoogleSQLEngineConn struct {
	conn     *sql.Conn
	tx       *sql.Tx
	analyzer *internal.Analyzer
}

func newGoogleSQLEngineConn(db *sql.DB, catalog *internal.Catalog, dialect internal.Dialect) (*GoogleSQLEngineConn, error) {
	conn, err := db.Conn(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to get database connection: %w", err)
	}
	analyzer, err := internal.NewAnalyzerWithDialect(catalog, dialect)
	if err != nil {
		return nil, fmt.Errorf("failed to create analyzer: %w", err)
	}
	return &GoogleSQLEngineConn{
		conn:     conn,
		analyzer: analyzer,
	}, nil
}

func (c *GoogleSQLEngineConn) SetAutoIndexMode(enabled bool) {
	c.analyzer.SetAutoIndexMode(enabled)
}

func (c *GoogleSQLEngineConn) SetExplainMode(enabled bool) {
	c.analyzer.SetExplainMode(enabled)
}

// SetMaxNamePath specifies the maximum value of name path.
// If the name path in the query is the maximum value, the name path set as prefix is not used.
// Effective only when a value greater than zero is specified ( default zero ).
func (c *GoogleSQLEngineConn) SetMaxNamePath(num int) {
	c.analyzer.SetMaxNamePath(num)
}

// MaxNamePath returns maximum value of name path.
func (c *GoogleSQLEngineConn) MaxNamePath() int {
	return c.analyzer.MaxNamePath()
}

// SetNamePath set path to name path to be set as prefix.
// If max name path is specified, an error is returned if the number is exceeded.
func (c *GoogleSQLEngineConn) SetNamePath(path []string) error {
	return c.analyzer.SetNamePath(path)
}

// NamePath returns path to name path to be set as prefix.
func (c *GoogleSQLEngineConn) NamePath() []string {
	return c.analyzer.NamePath()
}

// AddNamePath add path to name path to be set as prefix.
// If max name path is specified, an error is returned if the number is exceeded.
func (c *GoogleSQLEngineConn) AddNamePath(path string) error {
	return c.analyzer.AddNamePath(path)
}

func (c *GoogleSQLEngineConn) SetQueryParameters(parameters []*bigquery.QueryParameter) {
	c.analyzer.SetQueryParameters(parameters)
}

func (s *GoogleSQLEngineConn) CheckNamedValue(value *driver.NamedValue) error {
	return nil
}

func (c *GoogleSQLEngineConn) Prepare(query string) (driver.Stmt, error) {
	stmt, err := c.PrepareContext(context.Background(), query)
	return stmt, err
}

func (c *GoogleSQLEngineConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	conn := internal.NewConn(c.conn, c.tx)
	actionFuncs, err := c.analyzer.Analyze(ctx, conn, query, nil)
	if err != nil {
		return nil, err
	}
	var stmt driver.Stmt
	for _, actionFunc := range actionFuncs {
		action, err := actionFunc()
		if err != nil {
			return nil, err
		}
		s, err := action.Prepare(ctx, conn)
		if err != nil {
			return nil, err
		}
		stmt = s
	}
	return stmt, nil
}

func (c *GoogleSQLEngineConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (r driver.Result, e error) {
	conn := internal.NewConn(c.conn, c.tx)
	actionFuncs, err := c.analyzer.Analyze(ctx, conn, query, args)
	if err != nil {
		return nil, err
	}
	var actions []internal.StmtAction
	defer func() {
		eg := new(internal.ErrorGroup)
		eg.Add(e)
		for _, action := range actions {
			eg.Add(action.Cleanup(ctx, conn))
		}
		if eg.HasError() {
			e = eg
		}
	}()

	var result driver.Result
	for _, actionFunc := range actionFuncs {
		action, err := actionFunc()
		if err != nil {
			return nil, err
		}
		actions = append(actions, action)
		r, err := action.ExecContext(ctx, conn)
		if err != nil {
			return nil, err
		}
		result = r
	}
	return result, nil
}

func (c *GoogleSQLEngineConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (r driver.Rows, e error) {
	conn := internal.NewConn(c.conn, c.tx)
	actionFuncs, err := c.analyzer.Analyze(ctx, conn, query, args)
	if err != nil {
		return nil, err
	}
	var (
		actions []internal.StmtAction
		rows    *internal.Rows
	)
	defer func() {
		if rows != nil {
			// If we call cleanup action at the end of QueryContext function,
			// there is a possibility that the deleted table will be referenced when scanning from Rows,
			// so cleanup action should be executed in the Close() process of Rows.
			// For that, let Rows have a reference to actions ( and connection ).
			rows.SetActions(actions)
		}
	}()
	for _, actionFunc := range actionFuncs {
		action, err := actionFunc()
		if err != nil {
			return nil, err
		}
		actions = append(actions, action)
		queryRows, err := action.QueryContext(ctx, conn)
		if err != nil {
			return nil, err
		}
		rows = queryRows
	}
	return rows, nil
}

func (c *GoogleSQLEngineConn) Close() error {
	return c.conn.Close()
}

func (c *GoogleSQLEngineConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	tx, err := c.conn.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.IsolationLevel(opts.Isolation),
		ReadOnly:  opts.ReadOnly,
	})
	if err != nil {
		return nil, err
	}
	c.tx = tx
	return &GoogleSQLEngineTx{
		tx:   tx,
		conn: c,
	}, nil
}

func (c *GoogleSQLEngineConn) Begin() (driver.Tx, error) {
	tx, err := c.conn.BeginTx(context.Background(), nil)
	if err != nil {
		return nil, err
	}
	c.tx = tx
	return &GoogleSQLEngineTx{
		tx:   tx,
		conn: c,
	}, nil
}

type GoogleSQLEngineTx struct {
	tx   *sql.Tx
	conn *GoogleSQLEngineConn
}

func (tx *GoogleSQLEngineTx) Commit() error {
	defer func() {
		tx.conn.tx = nil
	}()
	return tx.tx.Commit()
}

func (tx *GoogleSQLEngineTx) Rollback() error {
	defer func() {
		tx.conn.tx = nil
	}()
	return tx.tx.Rollback()
}
