import lstore.config as config

class Record:

  def __init__(self, record_data, key):
    self.entire_record = record_data
    self.indirection = record_data[config.INDIRECTION_COLUMN]
    self.rid = record_data[config.RID_COLUMN]
    self.timestamp = record_data[config.TIMESTAMP_COLUMN]
    self.schema_encoding = record_data[config.SCHEMA_ENCODING_COLUMN]
    self.key = key
    self.columns = record_data[config.NUM_META_COLUMNS:]
