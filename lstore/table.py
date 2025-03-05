from lstore.bufferpool import BufferPool
from lstore.index import Index
from lstore.page_range import PageRange
from time import time
import threading
from lstore.page_range import PageRange

# 4 Meta Data Columns {
INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
# }

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key, bufferpool):
        self.name: str = name
        self.key = key
        self.num_columns = num_columns
        self.page_ranges_index = 0
        self.page_directory = {}
        self.index: Index = Index(self)
        self.rid = 1

        self.bufferpool: BufferPool = bufferpool
        pass

    def new_rid(self):
      tmp = self.rid
      self.rid += 1
      return tmp
      

    def add_new_page_range(self):
      if not self.bufferpool.get_frame(self.name, self.page_ranges_index, self.num_columns).page_range.has_base_page_capacity():
        self.page_ranges_index += 1

    def merge(self):
      merge_thread = threading.Thread(target=self.__merge, name="MergeThread")
      merge_thread.start()

    def __merge(self):
      new_page_directory = {}

      for pr in self.page_ranges:
        merged_records = []
        for base_page in pr.base_pages:
          for slot in range(base_page.num_records):
            record = base_page.read_record_at(slot, list(range(self.num_columns)))
            merged_records.append(record.copy())

        for tail_page in pr.tail_pages:
          tail_records = []
          for slot in range(tail_page.num_records):
            tail_records.append(tail_page.read_record_at(slot, list(range(self.num_columns))))
          for tail_record in reversed(tail_records):
            base_index = tail_record[RID_COLUMN]
            if tail_record[TIMESTAMP_COLUMN] > merged_records[base_index][TIMESTAMP_COLUMN]:
              for col in range(len(tail_record)):
                if tail_record[col] is not None:
                  merged_records[base_index][col] = tail_record[col]

        tail_timestamps = []
        for tail_page in pr.tail_pages:
          for slot in range(tail_page.num_records):
            tail_record = tail_page.read_record_at(slot, list(range(self.num_columns)))
            tail_timestamps.append(tail_record[TIMESTAMP_COLUMN])
        if tail_timestamps:
          new_tps = min(tail_timestamps)
        else:
          new_tps = merged_records[0][TIMESTAMP_COLUMN] if merged_records else 0
        pr.tps = new_tps

        new_page_directory[id(pr)] = merged_records

      self.page_directory.update(new_page_directory)