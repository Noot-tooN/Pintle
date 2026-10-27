//go:build (darwin || linux) && testhooks

package integration

import (
	"encoding/json"
	"os/exec"
	"path/filepath"
	"testing"
)

// Private local inspection, never a service endpoint. Each database is read in
// one SQLite transaction; callers park mutations before comparing snapshots.
const localDatabases = `
import {DatabaseSync} from 'node:sqlite';
import {readdirSync} from 'node:fs';
import {join, relative} from 'node:path';
const state = process.argv[1];
function files(dir) {
 return readdirSync(dir, {withFileTypes:true}).flatMap(e => e.isDirectory() ? files(join(dir,e.name)) : e.name.endsWith('.sqlite') ? [join(dir,e.name)] : []);
}
`

func localRecords(t *testing.T, root, state string) map[string]map[string]string {
	t.Helper()
	script := localDatabases + `
const result = {};
for (const path of files(state)) {
 const db = new DatabaseSync(path, {readOnly:true});
 // workerd can briefly lock SQLite for background storage work even while
 // domain mutations are parked. Bound the inspection wait; never skip a read.
 db.exec('PRAGMA busy_timeout=5000');
 if (db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='records'").get()) {
  db.exec('BEGIN');
  result[relative(state,path)] = Object.fromEntries(db.prepare('SELECT key,value FROM records ORDER BY key').all().map(r=>[r.key,r.value]));
  db.exec('COMMIT');
 }
 db.close();
}
console.log(JSON.stringify(result));`
	data, err := exec.Command(filepath.Join(root, "bin/node"), "--no-warnings", "--input-type=module", "-e", script, state).CombinedOutput()
	if err != nil {
		t.Fatalf("local SQLite inspection: %v: %s", err, data)
	}
	var rows map[string]map[string]string
	if err := json.Unmarshal(data, &rows); err != nil {
		t.Fatal(err)
	}
	if len(rows) == 0 {
		t.Fatal("no authority databases found")
	}
	return rows
}

// Boundary values cannot be reached by practical iteration. Seed one empty
// authority OFFLINE, after opening its session, then restart the actual Wasm.
func seedLocalCounters(t *testing.T, h *sessionHarness, fence, revision string) {
	t.Helper()
	if h.r.cmd != nil {
		t.Fatal("counter setup requires stopped Wrangler")
	}
	script := localDatabases + `
const paths = files(state).filter(path => {
 const db = new DatabaseSync(path, {readOnly:true});
 const yes = !!db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='records'").get();
 db.close(); return yes;
});
if (paths.length !== 1) throw Error('expected exactly one authority');
const db = new DatabaseSync(paths[0]);
db.exec('BEGIN IMMEDIATE');
if (db.prepare("SELECT key FROM records WHERE key IN ('owner','fence','revision')").get()) throw Error('expected unused mutex');
const insert = db.prepare('INSERT INTO records(key,value) VALUES (?,?)');
insert.run('fence',process.argv[2]); insert.run('revision',process.argv[3]);
db.exec('COMMIT'); db.close();`
	data, err := exec.Command(filepath.Join(h.root, "bin/node"), "--no-warnings", "--input-type=module", "-e", script, h.state, fence, revision).CombinedOutput()
	if err != nil {
		t.Fatalf("offline counter setup: %v: %s", err, data)
	}
}
