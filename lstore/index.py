from BTrees.OOBTree import OOBTree
import lstore.config as config

"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

# Some quick ideas...We know primary keys can be unique but if we choose to index other columns,
# there is no guarantee that the numbers there will be unique. For this reason, I believe that
# the Btree should store sets of RIDs as values. Since primary keys are unique, that's not needed but for other
# indexes it is.That way, you can iterate through each set if RIDs.
# For example, self.indices[1][10] = {20, 4, 19}. Here we are looking for records where the first column has a value of 10
# the records in question are stored in a set.

# The tree is made up of key, value pairs where the key is value in a column and value is an RID which
# can later be used by the page range.


class Index:
    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices: list = [None] * table.num_columns
        self.key = table.key
        self.table = table
        self.indices[self.key] = OOBTree()

    def add(self, record):
        """
        A method for adding something to the index. This method would most likely be used during the
        insert query. From the record the RID can be extracted and then for the columns that contain
        indexes, the data from that column can also be added into the Btree.
        """
        rid = record[config.RID_COLUMN]

        for i, column_index in enumerate(self.indices):
            # We have to do i + config.NUM_META_COLUMNS because the first config.NUM_META_COLUMNS columns are metadata columns. In other words, I am aligning
            key = record[i + config.NUM_META_COLUMNS]
            if column_index != None:
                # If we are going to allow duplicate values we should store RIDs in a set. RIDs are for certain unique
                if key not in column_index:
                    column_index[key] = set()
                # All nodes should contain sets
                column_index[key].add(rid)

    def delete(self, record):
        """
        This method would be used in the
        delete query. When a record is deleted you also call delete on the index. The RID is extracted
        and for each column that has an index, we search the index. For now this only works with unique primary keys
        """
        rid = record[config.RID_COLUMN]
        for i, column_index in enumerate(self.indices):
            if column_index != None:
                key = record[i + config.NUM_META_COLUMNS]
                if key in column_index:
                    # Recall the tree keys are SETs with RIDs
                    column_index[key].remove(rid)
                    # If the set is empty might as well delete thr key from the tree
                    if len(column_index[key]) == 0:
                        del column_index[key]
                else:
                    raise Exception("The key was not in the index")

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        # Search down an index for a specific set of RIDs. If it's not found, then it doesn't exist
        tree = self.indices[column]
        if value not in tree:
            return None

        return tree[value]  # For now this returns a set of RIDs

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        # This one confuses me a bit too. Not the most efficient way but we could start with an empty
        # array then we iterate from begin to end and append each of the RIDs in the set to this array.
        # By the end of the function we should have an array with all RIDs that were within the specified range.
        pass

    def to_arr(self):
      index_arr = []
      for index in self.indices:
        data = {}
        if index:

          for v, k in index.items():
            data[v] = list(k)

          index_arr.append(data)
        else:
          index_arr.append(None)

      return index_arr

    @classmethod
    def from_arr(cls, table, index_arr):
      index = Index(table)
      for i, data in enumerate(index_arr):
        if data:
          index.indices[i] = OOBTree()
          for k, v in data.items():
            index.indices[i][int(k)] = set(v)
      return index
    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
      # We are going to grab every thing that has been index, and use a query to get the latest version of the data
      # Then we put this version into the new indexed column

      # This needs to be done in here to prevent errors
      from lstore.query import Query
      query = Query(self.table)

      if (self.indices[column_number]):
        raise IndexError("An index for this column already exists")

      # Create the new Btree
      self.indices[column_number] = OOBTree()

      # Grab all the rids in the primary key
      for key in self.indices[self.key].keys():
        records = query.select(key, self.key, [1] * self.table.num_columns)
        for record in records:
          # This includes the meta_data columns which is why it looks so weird
          full_record = [record.indirection, record.rid, record.timestamp, record.schema_encoding]
          full_record = full_record + record.columns
          self.add(full_record)

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
      del self.indices[column_number]