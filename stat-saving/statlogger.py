import time
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
from shutil import copyfile
import os

def createFile(event):
  print(f"{event.src_path} has been modified!")
  filetype = event.src_path.split('/')[-1].split('.')[1]
  name= event.src_path.split('/')[-1].split('.')[0]

  if not os.path.exists(f'./stats/{name}/'):
    os.makedirs(f'./stats/{name}/')

  currentTime = int(time.time())
  copyfile(event.src_path, f'./stats/{name}/{currentTime}.{filetype}')


if __name__ == "__main__":
  patterns = "*"
  ignore_patterns = ""
  ignore_directories = True
  case_sensitive = True
  event_handler = PatternMatchingEventHandler(patterns, ignore_patterns, ignore_directories, case_sensitive)

  event_handler.on_created = createFile
  event_handler.on_modified = createFile

  path = '../minecraft/world/stats/'
  observer = Observer()
  observer.schedule(event_handler, path)

  observer.start()

  try:
    while True:
      time.sleep(1)
  except KeyboardInterrupt:
    observer.stop()
    observer.join()






