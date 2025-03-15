import os
import asyncio
import zipfile
import logging
from multiprocessing import Manager
from telethon import TelegramClient
from FastTelethon import upload_file,download_file
import time

from telethon import events, utils
from telethon.tl import types

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Telegram API credentials
api_id = "25997053"
api_hash = "e7d4c05f8304c9c6462fda50629b8918"
friend_myphone = '+91 9980315941'
my_phone = '+91 7975165908'

source_folder = "/media/satish/Data/MONGO DB BY DURGA SIR"
zip_folder = 'zip_files'
processed_files_log = 'processed_zips.csv'
MAX_ZIP_SIZE = 1.8 * 1024 * 1024 * 1024  # 2GB

client = TelegramClient('1789_satish', api_id, api_hash)

# Ensure zip folder exists
os.makedirs(zip_folder, exist_ok=True)


class Timer:
    def __init__(self, time_between=2):
        self.start_time = time.time()
        self.time_between = time_between

    def can_send(self):
        if time.time() > (self.start_time + self.time_between):
            self.start_time = time.time()
            return True
        return False
    
timer = Timer()

def get_processed_files():
    try:
        with open(processed_files_log, 'r') as file:
            return set(file.read().splitlines())
    except FileNotFoundError:
        return set()

def store_history(file_path):
    with open(processed_files_log, 'a') as file:
        file.write(f"{file_path}\n")

async def progress_bar(current, total):
        if timer.can_send():
            await print("{} {}%".format("file upload", current * 100 / total))

def create_zip_chunks(folder_path):
    base_name = os.path.basename(folder_path)
    zip_files = []
    part = 1
    
    logging.info("Starting ZIP creation...")
    
    files = [os.path.join(folder_path, f) for f in os.listdir(folder_path)]
    chunk_size = 0
    zip_file_path = os.path.join(zip_folder, f"{base_name}_part{part}.zip")
    zipf = zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED)
    
    for file in files:
        file_size = os.path.getsize(file)
        if chunk_size + file_size > MAX_ZIP_SIZE:
            zipf.close()
            zip_files.append(zip_file_path)
            logging.info(f"Created ZIP: {zip_file_path}")
            
            part += 1
            zip_file_path = os.path.join(zip_folder, f"{base_name}_part{part}.zip")
            zipf = zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED)
            chunk_size = 0
        
        zipf.write(file, os.path.basename(file))
        chunk_size += file_size
    
    zipf.close()
    zip_files.append(zip_file_path)
    logging.info(f"Created ZIP: {zip_file_path}")
    
    return zip_files

async def upload_worker(queue):
    await client.start(my_phone)
    friend = await client.get_entity(friend_myphone)
    logging.info("Upload worker started.")
    
    while True:
        zip_file_path = await queue.get()
        if zip_file_path is None:
            logging.info("No more files to upload. Exiting worker.")
            break  # Exit when done
        
        try:
            logging.info(f"Uploading: {zip_file_path}")

            # await client.send_file(friend, zip_file_path, caption=os.path.basename(zip_file_path))
            # msg = await event.reply("uploading started")
            with open(zip_file_path, "rb") as out:
                res = await upload_file(client, out, progress_callback=progress_bar)
                # result is InputFile()
                # you can add more data to it
                attributes, mime_type = utils.get_attributes(
                    zip_file_path,
                )
                media = types.InputMediaUploadedDocument(
                    file=res,
                    mime_type=mime_type,
                    attributes=attributes,
                    # not needed for most files, thumb=thumb,
                    force_file=False
                )
                # await msg.edit("Finished uploading")
                # await event.reply(file=media)
                # # or just send it as it is
                # await event.reply(file=res)

            logging.info(f"Upload completed: {zip_file_path}")
            store_history(zip_file_path)
            os.remove(zip_file_path)
        except Exception as e:
            logging.error(f"Upload failed for {zip_file_path}: {e}")
        finally:
            queue.task_done()

    await client.disconnect()
    logging.info("Telegram client disconnected.")

async def main():
    processed_files = get_processed_files()
    
    if source_folder in processed_files:
        logging.info("Skipping already processed folder.")
        return
    
    logging.info("Starting ZIP processing and upload...")
    
    manager = Manager()
    queue = asyncio.Queue()
    
    upload_task = asyncio.create_task(upload_worker(queue))
    
    zip_files = create_zip_chunks(source_folder)
    
    for zip_file in zip_files:
        logging.info(f"Adding {zip_file} to upload queue")
        await queue.put(zip_file)
    
    await queue.join()
    await queue.put(None)
    
    await upload_task
    store_history(source_folder)
    
    logging.info("Process completed successfully.")

if __name__ == "__main__":
    asyncio.run(main())