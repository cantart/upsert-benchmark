# upsert-benchmark

**Assumptions**
- Unknown schema at development time
- Insert or update decision required for each row

This repo explores different strategies to upsert large datasets efficiently into a database, without knowing the database schema at development time.

## 🧠 Strategies I am exploring

1. **Naive Upsert**
   - No indexing
   - Loop through each row → check → insert or update
   - Consumes a lot of CPU and memory

2. **Hash Index Upsert**
   - Use hash key as unique identifier
   - Enable fast conflict detection via index

3. **Batching + Hash Index Upsert**
   - Upsert in chunks to reduce memory usage and transaction cost

## 📊 How to run benchmark

Run:
```bash
go test -bench=. ./upsert
```

## 🚧 Future Plans

- Add runtime/adaptive tuning for batched upserts
