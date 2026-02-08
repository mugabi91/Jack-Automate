from typing import Any
import time
import json
from pandas.io.parsers.readers import TextFileReader
import pandas as pd
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from watchdog.events import FileSystemEvent
from docxtpl import DocxTemplate
from pathlib import Path
from rich import print as print
from threading import Timer

# Paths constants
BASE_DIR: Path = Path(__file__).parent
DATA_FILE: Path = BASE_DIR / "data" / "responses.xlsx"
TEMPLATE_FILE: Path = BASE_DIR / "templates" / "questionnaire_template.docx"
OUTPUT_DIR: Path = BASE_DIR / "output"
STATE_FILE: Path = BASE_DIR / "data" / "state.json"
OUTPUT_DIR.mkdir(exist_ok=True)

# DATASET SETTINGS
ID_COLUMN_NAME:str= 'BugReportID'
NAME_COLUMN_NAME:str= 'Name'

# state loader
def load_last_id():
    if not STATE_FILE.exists():
        return 0
    with open(STATE_FILE) as f:
        return json.load(f).get("last_processed_id",0)

# state saver
def save_last_id(last_id):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp: Path = STATE_FILE.with_suffix(".tmp")

    with open(tmp, "w") as f:
        json.dump({"last_processed_id": int(last_id)}, f)

    tmp.replace(STATE_FILE)  # atomic write

# event hanlder
class ExcelChangeHandler(FileSystemEventHandler):
    def __init__(self, debounce_seconds=3.0):
        self.debounce_seconds = debounce_seconds
        self._timer: Timer | None = None

    def _process(self):
        print("[bold green]Excel file stable. Generating documents...[/bold green]")
        generate()

    def on_modified(self, event: FileSystemEvent) -> None:
        if event.is_directory:
            return

        if Path(event.src_path).resolve() != DATA_FILE.resolve(): #type:ignore
            return

        print("[yellow]Excel modified, waiting for stability...[/yellow]")

        if self._timer:
            self._timer.cancel()

        self._timer = Timer(self.debounce_seconds, self._process)
        self._timer.start()


# Data loader
def load_data() -> TextFileReader | pd.DataFrame:
    if DATA_FILE.suffix in ["csv","txt"]:
        chunks: TextFileReader = pd.read_csv(filepath_or_buffer=DATA_FILE, chunksize=100_000)
        return chunks
    else:
        return pd.read_excel(io=DATA_FILE)

# data process/automation
def process(data) -> None:
    # iterate every row in the data set provided and create report
    for _, row in data.iterrows():
            filename = (f"BugReport_{row[ID_COLUMN_NAME]}_{row[NAME_COLUMN_NAME].strip().lower().replace(' ', '_')}.docx")
            output_file = OUTPUT_DIR / filename
            if not output_file.exists():
                doc = DocxTemplate(TEMPLATE_FILE)
                print(f"working on id {row[ID_COLUMN_NAME]}..") 
                context: dict[Any, Any] = row.to_dict()
                doc.render(context= context)            
                doc.save(filename=OUTPUT_DIR / filename)
                print(f"ID:{row[ID_COLUMN_NAME]} template created successfully") 
                print()

    print("All Bug reports generated successfully..")
    print(" \nTo stop the program hold click in the terminal and hold (control button + C button) x2")


def generate() -> None:
    # load data
    df: TextFileReader | pd.DataFrame = load_data()
    
    # track last id processed
    last_id = load_last_id()
    max_seen_id = last_id

    # check if its a dataframe or textfileReader
    if isinstance(df, TextFileReader):
        for chunk in df:
            if last_id is not None:
                chunk = chunk[chunk[ID_COLUMN_NAME] > last_id]
            if chunk.empty:
                continue
            process(data=chunk)
            max_seen_id = max(max_seen_id or 0, chunk[ID_COLUMN_NAME].max())
            
    elif isinstance(df, pd.DataFrame):
        process(data=df)
        max_seen_id = max(max_seen_id or 0, df[ID_COLUMN_NAME].max())

    save_last_id(last_id=max_seen_id)   
        
        
# program entry
def main() -> None:
    event_handler = ExcelChangeHandler()
    observer = Observer()
    observer.schedule(
        event_handler=event_handler,
        path=str(DATA_FILE.parent),
        recursive=False
    )

    observer.start()
    print("[bold cyan]Watching Excel file for changes...[/bold cyan]")
    print("[bold cyan]Go ahead and work..... watching for changes")
    print(" \nTo stop the program hold click in the terminal and hold (control button + C button) x2")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()


if __name__ == "__main__":
    main()