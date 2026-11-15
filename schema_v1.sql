PRAGMA foreign_keys=ON;
CREATE TABLE meta(version INTEGER NOT NULL CHECK(version=1));
INSERT INTO meta VALUES(1);
CREATE TABLE tasks(
 task_id TEXT PRIMARY KEY,
 signed_url TEXT NOT NULL,
 etag TEXT NOT NULL,
 committed INTEGER NOT NULL CHECK(committed>=0 AND committed%16=0)
);
CREATE TABLE warehouse(
 task_id TEXT NOT NULL REFERENCES tasks(task_id),
 ordinal INTEGER NOT NULL,
 observed INTEGER NOT NULL,
 value INTEGER NOT NULL,
 PRIMARY KEY(task_id,ordinal)
);
