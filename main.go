// poolerrepro shows a connection pooler corrupting the reply to a pipelined
// prepare of many named statements that its backend already has prepared.
//
//	go run . -url 'postgresql://USER:PASSWORD@ep-XXXX-pooler.REGION.aws.neon.tech/DB?sslmode=require'
//
// It uses only system catalogs. Connection A sends a pgx batch of N distinct
// queries in pgx's default mode (named prepared statements): pgx pipelines
// Parse+Describe for all N, then Sync. Connection B, a new client connection,
// then sends the same batch. In transaction pooling B is given the backend A
// just released, which already has the N statements, so the pooler answers B's
// Parse messages itself.
//
// Expected: both batches succeed.
// Observed through the Neon pooler: B's reply contains a ParseComplete message
// (31 00 00 00 04) in the middle of a RowDescription message.
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
)

// Distinct statements with wide result descriptions.
var catalogs = []string{
	"pg_class", "pg_attribute", "pg_type", "pg_proc", "pg_index", "pg_constraint", "pg_namespace",
	"pg_database", "pg_operator", "pg_am", "pg_trigger", "pg_rewrite", "pg_collation", "pg_extension",
}

// tap records the bytes pgx reads from the connection.
type tap struct {
	r   io.Reader
	mu  sync.Mutex
	buf []byte
}

func (t *tap) Read(p []byte) (int, error) {
	n, err := t.r.Read(p)
	t.mu.Lock()
	t.buf = append(t.buf, p[:n]...)
	t.mu.Unlock()
	return n, err
}

func (t *tap) reset() { t.mu.Lock(); t.buf = nil; t.mu.Unlock() }

func (t *tap) bytes() []byte {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]byte(nil), t.buf...)
}

func main() {
	url := flag.String("url", os.Getenv("DATABASE_URL"), "connection string (or DATABASE_URL)")
	mode := flag.String("mode", "cache_statement", "pgx exec mode: cache_statement (default, named statements), cache_describe, describe_exec, exec")
	n := flag.Int("n", len(catalogs), "number of statements in the batch")
	rounds := flag.Int("rounds", 3, "how many fresh client connections send the batch after connection A")
	flag.Parse()
	if *url == "" {
		fmt.Fprintln(os.Stderr, "set -url or DATABASE_URL")
		os.Exit(2)
	}
	ctx := context.Background()

	connect := func() (*pgx.Conn, *tap) {
		cfg, err := pgx.ParseConfig(*url)
		must(err)
		switch *mode {
		case "cache_statement":
			cfg.DefaultQueryExecMode = pgx.QueryExecModeCacheStatement
		case "cache_describe":
			cfg.DefaultQueryExecMode = pgx.QueryExecModeCacheDescribe
		case "describe_exec":
			cfg.DefaultQueryExecMode = pgx.QueryExecModeDescribeExec
		case "exec":
			cfg.DefaultQueryExecMode = pgx.QueryExecModeExec
		default:
			must(fmt.Errorf("unknown -mode %q", *mode))
		}
		t := &tap{}
		cfg.BuildFrontend = func(r io.Reader, w io.Writer) *pgproto3.Frontend {
			t.r = r
			f := pgproto3.NewFrontend(t, w)
			// Without a limit pgx would try to allocate the bogus 4 GiB body.
			f.SetMaxBodyLen(16 << 20)
			return f
		}
		conn, err := pgx.ConnectConfig(ctx, cfg)
		must(err)
		return conn, t
	}

	sendBatch := func(conn *pgx.Conn, t *tap) (int, error) {
		var pid int
		must(conn.QueryRow(ctx, "select pg_backend_pid()", pgx.QueryExecModeSimpleProtocol).Scan(&pid))
		t.reset()
		b := &pgx.Batch{}
		for _, c := range catalogs[:*n] {
			b.Queue("select * from pg_catalog."+c+" limit $1", int64(0))
		}
		cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		return pid, conn.SendBatch(cctx, b).Close()
	}

	var version string
	a, tapA := connect()
	must(a.QueryRow(ctx, "select version()", pgx.QueryExecModeSimpleProtocol).Scan(&version))
	fmt.Printf("server: %s\nmode: %s, statements per batch: %d\n\n", version, *mode, *n)

	pid, err := sendBatch(a, tapA)
	fmt.Printf("connection A (backend %d): %s\n", pid, result(err))

	failures := 0
	for i := 1; i <= *rounds; i++ {
		b, tapB := connect()
		pid, err := sendBatch(b, tapB)
		fmt.Printf("connection B%d, a new client connection (backend %d): %s\n", i, pid, result(err))
		if err != nil {
			failures++
			if failures == 1 {
				explain(tapB.bytes())
			}
		}
		_ = b.Close(ctx)
	}
	fmt.Printf("\n%d of %d new client connections failed\n", failures, *rounds)
	if failures > 0 {
		os.Exit(1)
	}
}

// explain walks the message framing of the reply and shows where it breaks.
func explain(data []byte) {
	const valid = "RK23CdcfGHWDIEVnNAtS1sZTv"
	names := map[byte]string{'1': "ParseComplete", 't': "ParameterDescription", 'T': "RowDescription", 'n': "NoData", 'Z': "ReadyForQuery"}
	pos, lastStart, lastType := 0, -1, byte(0)
	var seq strings.Builder
	for pos+5 <= len(data) {
		typ, length := data[pos], int(binary.BigEndian.Uint32(data[pos+1:pos+5]))
		if !strings.ContainsRune(valid, rune(typ)) || length < 4 || length > 1<<20 {
			fmt.Printf("\n  reply so far, by message type: %s\n", seq.String())
			fmt.Printf("  at byte %d of the reply the next header is type 0x%02x, length 0x%08x: not a protocol message\n", pos, typ, uint32(length))
			if lastType == 'T' {
				body := data[lastStart:min(pos+16, len(data))]
				if i := bytes.LastIndex(body, []byte{'1', 0, 0, 0, 4}); i >= 0 {
					at := lastStart + i
					fmt.Printf("  the preceding %s starts at byte %d and says it is %d bytes long;\n", names[lastType], lastStart, pos-lastStart)
					fmt.Printf("  a ParseComplete (31 00 00 00 04) sits inside it at byte %d:\n\n", at)
					lo, hi := max(at-32, 0), min(at+32, len(data))
					for _, l := range strings.Split(strings.TrimRight(hex.Dump(data[lo:hi]), "\n"), "\n") {
						fmt.Printf("    %s\n", l)
					}
					fmt.Printf("    (dump starts at byte %d of the reply)\n\n", lo)
				}
			}
			return
		}
		if pos+1+length > len(data) {
			break
		}
		seq.WriteByte(typ)
		lastStart, lastType = pos, typ
		pos += 1 + length
	}
	fmt.Printf("\n  the recorded reply framed cleanly: %s\n\n", seq.String())
}

func result(err error) string {
	if err == nil {
		return "ok"
	}
	return "FAILED: " + err.Error()
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(2)
	}
}
