# Transactions

Queues have a transactional interface, though the semantics may vary depending on the implementation. Using the default SQL-backed queues, transactions are ACID-compliant.

You can define transactions this way:

```python
from pipeteer import Transaction

async with Transaction(Qin, Qout) as tx:
  await Qout.push('key', 'value')
  await Qin.pop('key')
  await tx.commit()
```

You can add as many queues as you want to the transaction, and also autocommit it:

```python
async with Transaction(Q1, Q2, Q2, autocommit=True):
  ...
```