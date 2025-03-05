from lstore.page import Page  # Assuming your Page class is defined in page.py
import json
from lstore.config import NUM_META_COLUMNS

class ConceptualPage:
  def __init__(self, num_columns):
    """
    Initialize a base page with metadata and column pages.
    :param num_columns: Number of data columns (excluding metadata columns).
    """
    self.num_records = 0  # Tracks the total number of records, also treated as next available slot
    self.metadata_columns = NUM_META_COLUMNS  # Fixed number of metadata columns
    self.regular_columns = num_columns
    self.total_columns = self.metadata_columns + self.regular_columns
    # For the sake of consistency, the first four columns should be the metadata columns
    # Since everything is lazy, we start off with one row of column pages and expand until we reach 16
    # BasePages holds 4096 records = 512 records * 8 physical pages PER COLUMN
    self.pages = [[Page()] for _ in range(self.total_columns)]

  def __allocate_new_physical_pages(self):
    # First check is so we don't go over 8 ConceptualPages
    # The second check makes sure the next_slot (num_records) can't be stored in a physical page we already have
    physical_page_level = self.num_records // 512
    if physical_page_level <= 7 and physical_page_level >= len(self.pages[0]):
      for column in self.pages:
        column.append(Page())

  def has_capacity(self):
    """
    Checks if the base page has enough space for a new record.
    """
    return self.num_records < 4096


  def read_record_at(self, slot, projected_columns_index):
    """
    Returns an array based off of the projected columns index. This does not return any metadata. Just the record
    """
    if slot < 0 or slot >= self.num_records:
      raise IndexError("No record to read in that slot")

    if len(projected_columns_index) != self.regular_columns:
      raise Exception(f"In ConceptualPage, expected projected columns index of size #{self.regular_columns}, not #{len(projected_columns_index)}")

    # Project columns index is an array of 1s and 0s. 1 is a column you want
    # 0 is a column you don't want
    physical_page_level = slot // 512
    physical_page_slot = slot % 512
    record = []
    # Ignore the metadata columns
    for i in range(self.metadata_columns, self.total_columns):
      # If true we want this column, we subtract because the project_columns array starts at 0
      if projected_columns_index[i - self.metadata_columns]:
        record.append(self.pages[i][physical_page_level].read(physical_page_slot))
      else:
        record.append(None)

    return record

  def read_metadata_at(self, slot):
    """
    Specifically only for reading the metadata of a record. Useful if you need to update something
    like the indirection column or schema encoding.
    """
    physical_page_level = slot // 512
    physical_page_slot = slot % 512
    record = []
    for i in range(self.metadata_columns):
      # the first NUM_META_COLUMNS columns are the metadata columns
      record.append(self.pages[i][physical_page_level].read(physical_page_slot))

    return record

  def write_record(self, record):
    """
      Adding a new record in the Conceptual Page. This can be for base pages or tail pages. So please be sure
      of which pages you are writing into. This should take in data for the metadata columns as well
    """
    if not self.has_capacity():
      raise IndexError("Conceptual Page is full")

    if len(record) != self.total_columns:
      raise Exception("Not enough columns")

    new_slot = self.num_records
    physical_page_level = new_slot // 512
    physical_page_slot = new_slot % 512

    for column_num, data in enumerate(record):
      self.pages[column_num][physical_page_level].write(data, physical_page_slot)

    self.num_records += 1
    self.__allocate_new_physical_pages() # In case you fill the physical page up

  def update_column(self, column ,slot, new_indirection):
    """
    Should primarily be used for updating the indirection and schema encoding columns.
    Use carefully because we aren't supposed to update data in place for most cases
    """
    if slot < 0 or slot > self.num_records:
      raise IndexError("Invalid slot trying to be accessed")

    physical_page_level = slot // 512
    physical_page_slot = slot % 512
    self.pages[column][physical_page_level].write(new_indirection, physical_page_slot)


  def to_dict(self):
    # File will be overwritten if it exists
    data = {}

    # Some meta data stuff {
    data["regular_columns"] = self.regular_columns
    data["metadata_columns"] = self.metadata_columns
    data["num_records"] = self.num_records
    # }

    for i, column in enumerate(self.pages):
      data[str(i)] = {}

      for j, physical_page in enumerate(column):
        data[str(i)][str(j)] = physical_page.to_dict()
    return data

  @classmethod
  def from_dict(cls, data):
    new_conceptual_page = cls(data["regular_columns"])
    new_conceptual_page.metadata_columns = data["metadata_columns"]
    new_conceptual_page.num_records = data["num_records"]

    pages = []
    for column in range(new_conceptual_page.total_columns):
      pages.append([])
      for page in data[str(column)]:
        pages[column].append(Page.from_dict(data[str(column)][str(page)]))


    new_conceptual_page.pages = pages
    return new_conceptual_page

  def dump_file(self, name):
    with open(f"{name}.json", "w") as file:
      json.dump(self.to_dict(), file, indent=4)


  @classmethod
  def load_file(cls, json_file_name):
    data = {}
    with open(f"{json_file_name}.json") as file:
      data = json.load(file)


    regular_columns = data["regular_columns"]
    new_conceptual_page = cls(regular_columns)
    new_conceptual_page.metadata_columns = data["metadata_columns"]
    new_conceptual_page.num_records = data["num_records"]

    pages = []
    for column in range(new_conceptual_page.total_columns):
      pages.append([])
      for page in data[str(column)]:
        json_str = json.dumps(data[str(column)][str(page)])
        pages[column].append(Page.from_json_string(json_str))


    new_conceptual_page.pages = pages
    return new_conceptual_page