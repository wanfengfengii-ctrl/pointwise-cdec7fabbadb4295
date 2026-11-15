-- Public target schema. No migration or SDK2 adapter is implemented here.
CREATE TABLE format2(version INTEGER NOT NULL CHECK(version=2));
CREATE TABLE tasks2(
 task_id TEXT PRIMARY KEY,
 container TEXT NOT NULL,
 object_key TEXT NOT NULL,
 etag TEXT NOT NULL,
 committed INTEGER NOT NULL CHECK(committed>=0 AND committed%16=0)
);
CREATE TABLE warehouse2(
 task_id TEXT NOT NULL REFERENCES tasks2(task_id),
 ordinal INTEGER NOT NULL,
 observed INTEGER NOT NULL,
 value INTEGER NOT NULL,
 PRIMARY KEY(task_id,ordinal)
);
