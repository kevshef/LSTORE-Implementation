import os
import pickle
import time
import json
from typing import Optional

from lstore.page_range import PageRange


class Frame:
    def __init__(
        self,
        table_name: str,
        position: int,
        page_range: PageRange,
        from_disk: bool = False,
    ):
        self.is_dirty: bool = not from_disk # Pages not read from disk should be dirty so that they are written
        self.request_count: int = 0
        self.last_accessed = time.time()
        self.table_name: str = table_name
        self.position: int = position
        self.pin = 0

        self.page_range: PageRange = page_range

    def __enter__(self):
        self.pin += 1
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.pin -= 1


class BufferPool:
    def __init__(self, path):
        self.frame_request_count = 0  # For Most Used
        self.frames: dict[str, list[Frame]] = {}
        self.capacity = 16  # M3: Do we need more frames?
        self.path = path
        self.next_file_name = 0

        table_path = os.path.join(self.path, "tables")
        self.table_range_start_indices = {}
        if os.path.exists(table_path):
            for table_name in os.listdir(table_path):
                table_dir = os.path.join(table_path, table_name)
                if os.path.isdir(table_dir):
                    indices = [
                        int(f.split(".")[0])
                        for f in os.listdir(table_dir)
                        if f.endswith(".json") and f.split(".")[0].isnumeric()
                    ]
                    index = max(indices, default=-1)
                    if index > -1:
                        self.table_range_start_indices[table_name] = index

        pass

    def get_frame(self, table_name: str, page_range_index: int, num_columns: int):
        # TODO: HAS CAPACITY if not evict
        # TODO: Diego: Very incomplete, list needs to expand to right index and table key needs to be put in at some point possibly here along with an empty expandable list
        self.frames.setdefault(table_name, [])
        if table_name in self.frames:
            frames: list[Frame] = self.frames[table_name]
            append_count = max(0, page_range_index - len(frames) + 1)
            frames.extend([None] * append_count)
            frame: Frame = frames[page_range_index]
            if not frame:
                self.read_frame(table_name, page_range_index, num_columns)
                frame = frames[page_range_index]
                
            frame.request_count += 1
            self.last_accessed = time.time()
            return frame
        else:
            # TODO OOPS
            return None

    def has_capacity(self):
        sum = 0
        for table_frames in self.frames.values():
            for frame in table_frames:
                if frame:
                    sum += 1
        
        return sum <= self.capacity

    def read_frame(self, table_name: str, page_range_index: int, num_columns: int):
        # TODO Look into possible issues with this way of reading
        read_path = os.path.join(
            self.path, "tables", table_name, f"{page_range_index}.json"
        )
        # self.evict_frame()
        if os.path.exists(read_path):
            try:
                with open(read_path, "rb") as file:
                    self.frames[table_name][page_range_index] = Frame(
                        table_name,
                        page_range_index,
                        PageRange.from_dict(json.load(file)),
                        from_disk=True,
                    )
            except Exception as e:
                print(f"Exception raised while reading frame from disk: {e}")
        else:
            self.frames[table_name][page_range_index] = Frame(
                table_name, page_range_index, PageRange(num_columns)
            )

    def write_frame(self, frame: Frame):
        # TODO Look into possible issues with this way of writing
        write_path = os.path.join(self.path, "tables", frame.table_name)
        os.makedirs(write_path, exist_ok=True)
        if os.path.exists(write_path):
            frame_path = os.path.join(write_path, f"{frame.position}.json")
            if os.path.exists(frame_path):
                # print("Overwrite Log")
                pass
            try:
                with open(frame_path, "w", encoding="utf-8") as file:
                    json.dump(frame.page_range.to_dict(), file)
            except Exception as e:
                print(f"Exception raised while writing frame to disk: {e}")
        else:
            print("Somehow the write path didn't exist after making it.")
            pass

    def __oldget_least_needed_frame(self) -> Optional[Frame]:
        all_frames: list[Frame] = []
        for key in self.frames.keys:
            all_frames.extend(self.frames[key])
        sorted_frames = sorted(
            all_frames, key=Frame.request_count
        )  # Frames sorted from least requested to most requested
        stop_index = 0
        least_requests = 0
        for i, frame in enumerate(sorted_frames):
            if i == 0:
                least_requests = frame.request_count
            if frame.request_count > least_requests:
                stop_index = i
                break
        sorted_frames = sorted(
            sorted_frames[0:stop_index], key=Frame.last_accessed
        )  # Least requested pages sorted from oldest access to most recent access
        return sorted_frames[0] if sorted_frames else None
    
    def get_least_needed_frames_first(self) -> list[Frame]:
        all_frames: list[Frame] = []
        for key in self.frames.keys():
            all_frames.extend(self.frames[key])
            
        sorted_frames = sorted(all_frames, key=lambda frame: (frame.pin, frame.last_accessed, frame.request_count))
        stop_index = 0
        least_requests = 0
        for i, frame in enumerate(sorted_frames):
            if i == 0:
                least_requests = frame.request_count
            if frame.request_count > least_requests:
                stop_index = i
                break
        sorted_frames = sorted(
            sorted_frames[0:stop_index], key=Frame.last_accessed
        )  # Least requested pages sorted from oldest access to most recent access
        return sorted_frames

    # Evict Frame aka a Page Range with the Frame Data
    def evict_frame(self):
        # If Frame Dirty, write to Disk
        if self.frames and not self.has_capacity():
            frames = self.get_least_needed_frames_first()
            if not frames:
                raise Exception("No frames to evict.")
            frame = frames[0]
            if frame.is_dirty and not frame.pin:
                if frame.table_name in self.frames:
                        self.write_frame(frame)
                        self.frames[frame.table_name][frame.position] = None
            else:
                raise Exception("Failed to evict.")

    def on_close(self):
        for table_frames in self.frames.values():
            for frame in table_frames:
                if frame.is_dirty:
                    self.write_frame(frame)
            
