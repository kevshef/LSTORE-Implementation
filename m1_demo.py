from ipaddress import summarize_address_range

from lstore.db import Database
from lstore.query import Query
from random import randint
import time

db = Database()

# Let's create a table with 4 columns, where the first column is the primary key
grades_table = db.create_table('Demo', 4, 0)

query = Query(grades_table)

# Don't be afraid to scale this number up!
num_records = 1000
start = time.time()

for i in range(num_records):
  query.insert(*[i, randint(0,50), randint(0,50), randint(0,50)])

print("Insertion finished")

# Select any key from 0, 1000 and see what was inserted
# records = query.select(150, 0, [1, 1, 1, 1])
# for record in records:
#   print(record.columns)

# Let's change all the columns into 0s (except the pk)
for i in range(num_records):
  query.update(i,*[i, 0, 0, 0])

# Select any key from 0, 1000 and see what was updated. They should all be the same!
# records = query.select(150, 0, [1, 1, 1, 1])
# for record in records:
#   print(record.columns)

# Change this to the primary key column and see how the number changes!
summation = query.sum(1, 1000, 1)
print(summation)

# Now let us delete all the records
for i in range(num_records):
  query.delete(i)
  records = query.select(i, 0, [1,1,1,1])

  if len(records) != 0:
    print("The record was not deleted")

end = time.time()
print("All of this took: ", end - start, "seconds. With", num_records, "records.")




